#!/usr/bin/python3

import lxml.etree as ET
import time
import psycopg2
import typing
import asyncio

from . import settings
from . import dbutils
from . import log
from . import __version__

"""
FUTURE IMPROVEMENTS
    add support WITHOUT planet_bin_nodes... try a test database port 5433 for that
        -> generate fantasy nodes out of geometries? (fantasy IDs I mean, latlon from
            ST_Dump(ST_Transform(way,4326)).points...)
        -> out of xml-order generation!? first ways, rels and then "create" all missing tagless nodes
        -> would need even more RAM
    rewrite in C++, haha but would atleast be more RAM-efficient... also xml library can do by hand (CDATA[[]])
"""

class Accumulator() :

    def g_adaptive_parent_multiquery(self,name:str,c:psycopg2.extensions.cursor,
            queries:typing.Collection[str],
            nodelist_lambda_tuples:typing.Collection[typing.Collection[typing.Callable]]
        )->typing.Iterator :
        ''' For all the ids referred to by name :
        The database has some indexes on the bigint[] columns that contain
        arrays of children elements. We want to query those in reverse: which
        are all the parents of a given element ?
        To do this, run multiple queries for each child element's id.
        More specifically, the child_id ALWAYS appears in an array, eg:
            ... WHERE ARRAY[child_id]::bigint[] <@ nodes
            ... WHERE ARRAY['n'||child_id] <@ members
        In that case, and with the assumption that we do not care about _which_
        parent belongs to which child, we can run the queries "in parallel" :
            ... WHERE ARRAY[child_id1,child_id2,..]::bigint[] && nodes
            ... WHERE ARRAY['n'||child_id1,'n'||child_id2,...] && members
        This is a marked throughput improvement, but the performance also drops
        dramaticaaly once we surpass the indexed array size. on my machine it seems
        to be 11. But this function will readjust this chunk_size dynamically,
        to allow for the highest available database performance.
        Return format: generate tuples of a first int and then (lists of tuples) as
        (scanned_nodes_count,queries[0].results,queries[1].results,...)
        where queries[0].results=[row1_tup,row2_tup,row3_tup],
        len(q[0].res) is not necessarily equal to len(q[1].res), and
        scanned_nodes_count it the amount of nodes processed by this tuple.
        '''
        nix=self.named_data.index(name)
        len_ids=self.len(name)

        total_processed_nodes=0
        start_chunk_size=50 #const
        chunk_size=start_chunk_size
        avg_accepted_chunk_size=0
        chunks_unchanged_chunk_size=0
        stable=False
        chunks=0
        # to trigger a QueryCanceled when the index could not be used and the query
        # took too long
        c.execute("SET statement_timeout='1s';")
        printed_slow_warning=False
        ids_as_list=list(self.data[name])
        while total_processed_nodes<len_ids :
            start_time=time.time()
            #nodes_chunk=self.get_iter_slice(nix,total_processed_nodes,total_processed_nodes+chunk_size)
            nodes_chunk=ids_as_list[total_processed_nodes:total_processed_nodes+chunk_size]
            # maybe rewrite logic to NOT NEED len(nodes_chunk), then we could
            # have it be a generator
            # but we need to store the str=','.join(ids) anyways...
            nodes_chunk=list(nodes_chunk) #less efficient that way
            try :
                results=[None for q in queries]
                for ix,q in enumerate(queries) :
                    #apply callbacks
                    data_s=list(map(lambda f:f(nodes_chunk),nodelist_lambda_tuples[ix]))
                    q_uery=q.format(*data_s)
                    #l.log(q_uery)
                    c.execute(q_uery)
                    results[ix]=list(dbutils.g_from_cursor(c))
                total_processed_nodes+=len(nodes_chunk)
                #tentatively highten the chunk size, just to spice things up
                # (and maybe the db engine has warmed up in the meantime)
                if chunks_unchanged_chunk_size>(2500 if stable else 50):
                    #in the case we may hit the ceiling of index-performs-poorly
                    if chunk_size-0.49<=avg_accepted_chunk_size<=chunk_size+0.49 :
                        chunk_size+=0 #pass, but still set chunks_unchanged_chunk_size 0
                    else :
                        chunk_size+=1
                    #in the case we hit the avg_accepted_chunk_size ceiling, limitied by the initial
                    # start_chunk_size :
                    # check if avg was "dragged down" by a too-low initial chunk_size
                    #if start_chunk_size-2<=avg_accepted_chunk_size :
                    #    chunk_size=int((chunk_size*1.1)+1)
                    chunks_unchanged_chunk_size=0
                else :
                    chunks_unchanged_chunk_size+=1
                #total chunk averag is sum(all_accepted_chunks)/count(all_accepted_chunks)
                if chunks!=0 :
                    avg_accepted_chunk_size=total_processed_nodes/chunks
                yield (len(nodes_chunk),*results)
                chunks+=1
            except psycopg2.errors.QueryCanceled :
                # DO NOT increment total_processed_nodes, because we need to redo the work
                #TODO: chunk_size, can it also grow ?
                prev_chunk_size=chunk_size
                chunks_unchanged_chunk_size=0
                if chunk_size-2<=avg_accepted_chunk_size<=chunk_size-1 :
                    chunk_size-=1
                    stable=True
                else :
                    chunk_size=int(chunk_size/1.2)
                    if prev_chunk_size==chunk_size :
                        chunk_size-=1
                    if chunk_size==0 :
                        chunk_size=1 #nothing to be done...
                c.execute('ABORT;') #start new
                if chunk_size==0 :
                    chunk_size=1 #well we just need to work with the slow database...
                    if not printed_slow_warning :
                        log.l.log('WARNING: queries are running very slowly, the index may not exist.')
                        log.l.log(f'\tplease kill this process: "kill {log.l.pid}" and create indexes:')
                        log.l.log('\tCREATE INDEX planet_osm_ways_nodes_bucket_idx ON planet_osm_ways')
                        log.l.log('\t\tUSING GIN (planet_osm_index_bucket(nodes))')
                        log.l.log('\t\tWITH (fastupdate = off);')
                        printed_slow_warning=True
                    #see below, db forgets it. but don't rely on its forgetfulness
                    c.execute("SET statement_timeout=0;")
                else :
                    #it forgets that at each new transaction
                    c.execute("SET statement_timeout='1s';")
        #set it back, maybe run an ABORT; instead ?
        c.execute("SET statement_timeout='2h';")

class DictAccumulator(Accumulator) :
    def __init__(self,named_data) :
        self.named_data=named_data
        self.data={k:set() for k in self.named_data}

    def add(self,k,i) :
        assert i>0 and isinstance(i,int), f'Unsupported type or zero or negative value {i}'
        self.data[k].add(i)
    def all(self,k) :
        return iter(self.data[k])
    def is_in(self,k,i) :
        return i in self.data[k]
    def len(self,k) :
        return len(self.data[k])
    def clear(self,k) :
        del self.data[k]
        self.data[k]=set()
    def get_iter_slice(self,k,start,end) :
        l=list(self.data[k])
        return l[start:end]
    def all_subtract(self,k_from,k_remove) :
        missing=set()
        for i in self.data[k_from] :
            if i not in self.data[k_remove] :
                missing.add(i)
        return iter(missing)


async def chain(*generators:typing.Iterator)->typing.Iterator:
    for g in generators :
        if not hasattr(g,'__anext__') :
            #async does not like 'yield from' syntax, but this works
            for i in g :
                yield i
        else :
            #is an asyncio generator
            async for i in g :
                yield i


def all_nwr_within(s:settings.Settings,a:Accumulator) :
    #SELECT workflow to get all element [ids ONLY] in bounding box or boundary:
    # 1a) select all nodes WHERE way ST_Within(bbox);
    constr,tbl_name=s.make_bounds_constr('_point')
    log.l.log('executing big query on',tbl_name,'...',clearline=True)
    s.c.execute(f'SELECT osm_id FROM {tbl_name} WHERE {constr};')
    for row in dbutils.g_from_cursor(s.c,verbose=True,prefix_msg=tbl_name+' ') :
        a.add('nodes',row['osm_id'])
    log.l.log(log.n(a.len('nodes')),'nodes within bounds')

    # 1b) select all ways,rels FROM planet_osm_polygon WHERE way ST_Within(bbox);
    constr,tbl_name=s.make_bounds_constr('_polygon')
    log.l.log('executing big query on',tbl_name,'...',clearline=True)
    s.c.execute(f'SELECT osm_id FROM {tbl_name} WHERE {constr};')
    for row in dbutils.g_from_cursor(s.c,verbose=True,prefix_msg=tbl_name+' ') :
        id=row['osm_id']
        if id>0 :
            a.add('ways',id)
        else :
            a.add('rels',-id)
    log.l.log(log.n(a.len('ways')),'ways,',log.n(a.len('rels')),'rels from',tbl_name)

    # 1c) select all ways,rels FROM planet_osm_line WHERE way ST_Within(bbox);
    # planet_osm_roads is not needed in that fashion, because it is a strict subset
    # of planet_osm_line
    constr,tbl_name=s.make_bounds_constr('_line')
    log.l.log('executing big query on',tbl_name,'...',clearline=True)
    s.c.execute(f'SELECT osm_id FROM {tbl_name} WHERE {constr};')
    for row in dbutils.g_from_cursor(s.c,verbose=True,prefix_msg=tbl_name+' ') :
        id=row['osm_id']
        if id>0 :
            a.add('ways',id)
        else :
            a.add('rels',-id)
    log.l.log(log.n(a.len('ways')),'ways,',log.n(a.len('rels')),'rels within bounds')

def nodes_parent_wr(s:settings.Settings,a:Accumulator,only_nodes_within=False) :
    # 2a) foreach node_id :
    # 2b) select all ways WHERE ARRAY[node_id]::bigint[] <@ nodes;
    # 2c) select all rels WHERE ARRAY[node_id]::bigint[] <@ parts;
    nodes_name='nodes_within' if only_nodes_within else 'nodes'
    a_len=a.len
    way_count=0
    rel_count=0
    node_count=0

    try :
        s.c.execute('SELECT planet_osm_index_bucket(ARRAY[]::bigint[])')
        use_bucket_func=True
    except psycopg2.errors.UndefinedFunction :
        use_bucket_func=False #does not exist
    if use_bucket_func :
        add_buck='planet_osm_index_bucket(ARRAY[{0}]::bigint[]) && planet_osm_index_bucket(nodes) AND '
    else :
        add_buck=''

    tbl_rels=s.tables['_rels']['name']
    tbl_ways=s.tables['_ways']['name']
    if s.new_jsonb_schema :
        # the ::char(1) cast IS IMPORTANT for index performance, 10x or more slower without it
        rels_query="SELECT id FROM "+tbl_rels
        rels_query+=" WHERE planet_osm_member_ids(members,'N'::char(1)) && ARRAY[{0}]::bigint[];"
        rels_lambdas=(lambda i:','.join(map(str,i)),)
    else :
        parts_indexed="SELECT id,members FROM "+tbl_rels
        parts_indexed+=" WHERE ARRAY[{0}]::bigint[] && parts"
        members_where="ARRAY[{1}] && members"
        rels_query=f'SELECT id FROM ({parts_indexed}) AS parts_indexed WHERE {members_where};'
        rels_lambdas=(lambda i:','.join(map(str,i)),lambda i:','.join(map(lambda j:f"'n{j}'",i)),)
    
    for node_c,way_ids,rel_ids in a.g_adaptive_parent_multiquery(nodes_name,s.c,
            ('SELECT id FROM '+tbl_ways+' WHERE '+add_buck+'ARRAY[{0}]::bigint[] && nodes;',
                rels_query),
            [(lambda i:','.join(map(str,i)),),rels_lambdas]) :
        node_count+=node_c
        log.l.doublerate(way_count,'ways',rel_count,'rels parents of node',node_count,a_len(nodes_name))
        for way in way_ids:
            way_count+=1
            a.add('ways',way['id'])
        for rel in rel_ids:
            rel_count+=1
            a.add('rels',rel['id'])
    log.l.finishrate()
    log.l.log(log.n(a_len('ways')),'ways,',log.n(a_len('rels')),'rels forward from nodes')

def ways_parent_r(s:settings.Settings,a:Accumulator) :
    # 3a) foreach way_id :
    # 3b) select all rels WHERE ARRAY[way_id]::bigint[] <@ parts;
    way_count=0
    rel_count=0
    tbl_rels=s.tables['_rels']['name']
    a_len=a.len
    if s.new_jsonb_schema :
        # the ::char(1) cast IS IMPORTANT for index performance, 10x or more slower without it
        rels_query="SELECT id FROM "+tbl_rels
        rels_query+=" WHERE planet_osm_member_ids(members,'W'::char(1)) && ARRAY[{0}]::bigint[];"
        rels_lambdas=(lambda i:','.join(map(str,i)),)
    else :
        parts_indexed="SELECT id,members FROM "+tbl_rels
        parts_indexed+=" WHERE ARRAY[{0}]::bigint[] && parts"
        members_where="ARRAY[{1}] && members"
        rels_query=f'SELECT id FROM ({parts_indexed}) AS parts_indexed WHERE {members_where};'
        rels_lambdas=(lambda i:','.join(map(str,i)),lambda i:','.join(map(lambda j:f"'w{j}'",i)),)

    for way_c,rel_ids in a.g_adaptive_parent_multiquery('ways',s.c,
            (rels_query,),[rels_lambdas]) :
        way_count+=way_c
        for rel in rel_ids:
            rel_count+=1
            a.add('rels',rel['id'])
        log.l.rate(rel_count,'rels parents of way',way_count,a_len('ways'))
    log.l.finishrate(lastline=False)
    log.l.log(a_len('rels'),'rels forward')

def rels_children_nwr(s:settings.Settings,a:Accumulator,only_multipolygon_rels=False,without_rels=False) :
    ''' Going over all rel ids in accumulator, read every rel's members[] array and add all its
    children, according to their type: node/way/relation, to the accumulator.
    without_rels==True means to disregard the rel's children that are rels.
    only_multipolygon_rels==True means to only scan rels that are type="multipolygon".
    '''
    # 4) BACKpropagation: resolve to take in all rels->ways->nodes
    # 4a) foreach rel_id: add all its members'ids as n{id} -> node, w{id} -> way, or r{id} -> rel
    node_count=0
    way_count=0
    rel_count=0
    tot_count=0
    a_len=a.len
    #relation can be of very different types. a multipolygon could be a forest for example,
    # with an associated geometry. constrast that to a type="route" or type="superroute",
    # thore are only groups of already representable ways on the map
    # see osm wiki /Types_of_relation for more
    if s.new_jsonb_schema :
        multipolygon_constr=" AND (tags->>'type')='multipolygon'"
    else :
        multipolygon_constr=" AND ((tags::hstore)->'type')='multipolygon'"
    multipolygon_constr=multipolygon_constr if only_multipolygon_rels else ''

    tbl_rels=s.tables['_rels']['name']
    for j in (1,2): #repeat twice to resolve rels that have rels as children
        buffer_add_rels=set()
        for rel_id in a.all('rels') :
            #for row in g_query_ids()
            tot_count+=1
            s.c.execute(f'SELECT members FROM {tbl_rels} WHERE id={rel_id}{multipolygon_constr};')
            for row in dbutils.g_from_cursor(s.c) :
                ms=row['members']
                if s.new_jsonb_schema :
                    for m in ms :
                        if m['type']=='N' :
                            a.add('nodes',m['ref'])
                            node_count+=1
                        elif m['type']=='W' :
                            a.add('ways',m['ref'])
                            way_count+=1
                        elif m['type']=='R' :
                            buffer_add_rels.add(m['ref'])
                            if not without_rels :
                                rel_count+=1
                        else :
                            raise ValueError(f'''Encountered invalid members[]
                                element of rel id {rel_id} in members={repr(ms)},
                                value {repr(m)}''')
                else :
                    for i in range(0,len(ms),2) :
                        osm_type=ms[i][0]
                        osm_id=int(ms[i][1:])
                        if osm_type=='n' :
                            a.add('nodes',osm_id)
                            node_count+=1
                        elif osm_type=='w' :
                            a.add('ways',osm_id)
                            way_count+=1
                        elif osm_type=='r' :
                            buffer_add_rels.add(osm_id)
                            if not without_rels :
                                rel_count+=1
                        else :
                            raise ValueError(f'''Encountered invalid members[]
                                element of rel id {rel_id} in members={repr(ms)}
                                at index [{i}]={repr(ms[i])}''')
            log.l.triplerate(node_count,'nodes',way_count,'ways',rel_count,'rels children of rel',
                    tot_count,a_len('rels'))
        if without_rels :
            #l.save_clearedline()
            log.l.finishrate()
            return #after first run
        for rel_id in buffer_add_rels :
            a.add('rels',rel_id)
        tot_count=0 #reset counter to make only count up to 100% not 200%
    log.l.finishrate()

def ways_children_n(s:settings.Settings,a:Accumulator) :
    a_len=a.len
    # 4b) foreach way_id: add all its nodes[] ids
    way_count=0
    node_count=0
    for way_id in a.all('ways') :
        way_count+=1
        s.c.execute(f'SELECT id,nodes FROM {s.tables["_ways"]["name"]} WHERE id={way_id};')
        for row in dbutils.g_from_cursor(s.c) :
            for i in row['nodes'] :
                a.add('nodes',i)
                if i==122317 :
                    log.l.log('n122317 is in',a.is_in('nodes',122317))
                    log.l.log('FROM way',row['id'],'which has',len(row['nodes']),'nodes')
                node_count+=1
        log.l.rate(node_count,'nodes children of way',way_count,a_len('ways'))
    log.l.finishrate()


async def stream_osm_xml(s:settings.Settings) :
    ''' Query osm2pgsql-imported postgres database for nodes, ways and rels and stream
    an xml representation of them into s.out_file. Attempts to select objects that are in
    the given bounds. But the dependencies are sometimes required, so more data that just
    within the bounds will be included. Currently, no geometric features are clipped in
    any way.
    See --help for s.bounds.
    '''
    log.l.log_start(time.strftime('%F_%T'))
    ## TODO: move this config to Settings
    with_parents=True
    phases=['within','children','parents','write']
    if not with_parents :
        phases.remove('parents')
    log.l.set_phases(phases)

    #nodes within are a subset of nodes: copy of nodes just after all_nwr_within was run
    a=DictAccumulator(('nodes','nodes_within','ways','rels','done_ids'))

    #NOTE: only nodes existing in _point are selected: they are
    #   about 5% of all nodes usually
    all_nwr_within(s,a)
    #copy [~100K tagged_nodes, ~300K ways, ~7K rels]
    for i in a.all('nodes') :
        a.add('nodes_within',i)

    log.l.next_phase() #children

    ## TODO: move config to Settings
    # [+0K nodes, +60K ways, +0K rels] only_multipolygon_rels=True,without_rels=True
    rels_children_nwr(s,a,only_multipolygon_rels=True,without_rels=True)
    # [+3.2M nodes]
    ways_children_n(s,a)
    # we now have: [~3.3M nodes, ~350K ways, ~7K rels]

    if with_parents :
        log.l.next_phase() #parents
        # [+40K ways, +1K rels]
        nodes_parent_wr(s,a,only_nodes_within=True)
        #ways_parent_r(s,a)

    log.l.next_phase() #write
    counts=[a.len(i)for i in ('nodes','ways','rels')]
    log.l.log('dumping',log.n(counts[0]),'nodes,',log.n(counts[1]),'ways,',log.n(counts[2]),'rels in total')
    # we now have: [~3.3M nodes, ~400K ways, ~8K rels] with_parents=True

    # ONLY after all ids have been resolved, do we actually query the data,
    # RAM-inefficient otherwise; more RAM-inefficient for bigger extracts.
    # do more of a streaming from database to file approach
    with ET.xmlfile(s.out_file,encoding='utf-8') as xml_out :
        xml_out.write_declaration()
        with xml_out.element('osm',{
            'version':'0.6',
            'generator':f'{__package__} v{__version__}',
            'at_time':time.strftime(f'%F_%T'),
            'url':s.project_url,
        }) :
            async for el in chain(
                    create_nodes(s,a),
                    create_ways(s,a),
                    create_relations(s,a),
            ) :
                xml_out.write(el)

def rel_to_xml(row_dict:dict,tags:dict,new_jsonb_schema:bool)->ET.Element :
    # separate tags and row_dict, see way_to_xml()
    attrs,col_tags=split_tags_out(row_dict,('id','members'))
    rel=ET.Element('relation',{'id':str(attrs['id'])})
    if 'members' in attrs :
        if new_jsonb_schema :
            for m in attrs.pop('members') :
                trsl={'N':'node','W':'way','R':'relation'}
                ET.SubElement(rel,'member',{'type':trsl[m['type']],'ref':str(m['ref']),'role':m['role']})
        else :
            m=iter(attrs.pop('members'))
            while True :
                try :
                    mixed_id=next(m)
                    role=next(m)
                    osm_type={'n':'node','w':'way','r':'relation'}[mixed_id[0]]
                    id=mixed_id[1:]
                    ET.SubElement(rel,'member',{'type':osm_type,'ref':str(id),'role':role})
                except StopIteration :
                    break
    have_keys=set()
    for t in (tags,col_tags) :
        for k,v in tags.items() :
            if k not in have_keys :
                # make sure a float like v=18.572e6 does not have the 'e' in str() -> YES: "18572000.0"
                ET.SubElement(rel,'tag',{'k':str(k),'v':str(v)})
                have_keys.add(k)
    return rel

def create_relations(s:settings.Settings,a:Accumulator)->typing.Iterator[ET.Element] :
    ''' Read all ids from accumulator, under a.all('rels') and fetch corresponding
    data from database.
    Use the accumulator 'done_ids' for checking what was done and what needs to be done.
    And use the accumulator 'missing_ids' for checking what still needs to be done.
    '''
    tbl_rels=s.tables['_rels']['name']
    a_add=a.add
    a_len=a.len
    add_done_ids=lambda i:a_add('done_ids',i)
    a.clear('done_ids')
    len_ids=a_len('rels')

    table_name=s.tables['_polygon']['name']
    log.l.log('reading table',table_name,'...')
    read_columns=[f'-{table_name}.osm_id AS id',
        f'hstore_to_json({table_name}.tags) AS json_tags',
        # we need to merge the _polygon tags with the _rels tags.
        # _planet tags are overwritten by _rels tags because
        # json_tags is first loaded, the json_tags2 overloads all existing and non-existing keys
        f'{tbl_rels}.tags AS json_tags2' if s.new_jsonb_schema else f'hstore_to_json({tbl_rels}.tags::hstore) AS json_tags2',
        # do we need this ? no ; especially the tags->'area'='yes' will overwrite 'area' if it exists
        #f'{table_name}.way_area AS area',
        #I think real does not exist and real->float4
        *list(dbutils.get_columns_of_types(s.c,('int4','int','int8','int16','text','real','float4','float8'),table_name))
    ]
    query='SELECT '+(','.join(read_columns))
    query+=f',{tbl_rels}.members FROM {table_name} JOIN {tbl_rels}'
    query+=f' ON -{table_name}.osm_id={tbl_rels}.id'

    # the JOIN makes this query incredibly slow...
    # BUT ONLY "ON osm_id=-id", and "ON -osm_id=id" is fast...

    # psql does not have an index on -osm_id and does not understand *=-1 is bijective.
    #therevore  checking -osm_id IN (id1,id2,id3) is super slow, but
    # osm_id IN (-id1,-id2,-id3) is fast. But it needs some more memory in python to
    # store the negatives copy as well
    if s.debug_xml :
        yield ET.Element('debug',{'status':'starting polygon query'})
    for row_dict in dbutils.g_query_ids(s.c,query,g_negate(a.all('rels')),'osm_id',step=250) :
        if a.is_in('done_ids',row_dict['id']) :
            continue
        #collapse hstore tags 
        tags=row_dict.pop('json_tags') if 'json_tags' in row_dict else {}
        tags={**tags,**row_dict.pop('json_tags2')} if 'json_tags2' in row_dict else tags
        yield rel_to_xml(row_dict,tags,s.new_jsonb_schema)
        if s.debug_xml :
            yield ET.Element('debug',{'previous':str(row_dict['id']),
                'done_ids_len':str(a_len('done_ids')),'ids_len':str(len_ids),
                'table':table_name})
        add_done_ids(row_dict['id'])
        log.l.simplerate(a_len('done_ids'),'rels',len_ids)
        #l.log(log.n(a_len('done_ids')),'/',log.n(len_ids),'rels','    ',
        #        percent(a_len('done_ids'),len_ids),clearline=True)
    log.l.finishrate()

    #and now with _line as well
    table_name=s.tables['_line']['name']
    log.l.log('reading table',table_name,'...')

    read_columns=[f'-{table_name}.osm_id AS id',
        f'hstore_to_json({table_name}.tags) AS json_tags',
        #f'{tbl_rels}.tags AS json_tags2' if s.new_jsonb_schema else f'hstore_to_json({tbl_rels}.tags::hstore) AS json_tags2',
        *list(dbutils.get_columns_of_types(s.c,('int4','int','int8','int16','text','real','float4','float8'),table_name))
    ]
    
    #TEMP: there seems to be no index on _line! so we do additional queries one by one when double_query_mode==True
    double_query_mode=True
    if not double_query_mode :
        read_columns.append(f'{tbl_rels}.members')
        if s.new_jsonb_schema :
            read_columns.append(f'{tbl_rels}.tags AS json_tags2')
        else :
            read_columns.append(f'hstore_to_json({tbl_rels}.tags::hstore) AS json_tags2')
    query='SELECT '+(','.join(read_columns))+f' FROM {table_name}'

    if not double_query_mode :
        # try an approach with ids substituted twice in, once positive and once negative
        # would g_adaptive_parent_multiquery work here ? it's not for parents though...
        # sloooow
        #query+=f' JOIN {tbl_rels} ON {table_name}.osm_id=-{tbl_rels}.id'
        #query+=f' JOIN {tbl_rels} ON -{table_name}.osm_id={tbl_rels}.id'
        # sloow
        query+=f', {tbl_rels} WHERE {table_name}.osm_id=-{tbl_rels}.id'
        #slooow
        #query+=f', {tbl_rels} WHERE -{table_name}.osm_id={tbl_rels}.id'
    else :
        # try a FROM _line,_rels WHERE -_line.osm_id=rels.id, maybe that recognizes the index ?
        if s.new_jsonb_schema :
            query2=f'SELECT tags AS json_tags2,members FROM {tbl_rels} WHERE id=' #need to append id
        else :
            query2=f'SELECT hstore_to_json(tags::hstore) AS json_tags2,members FROM {tbl_rels} WHERE id=' #need to append id
        cursor2=s.access.cursor() # need a second cursor here

    # psql does not have an index on -osm_id and does not understand *=-1 is bijective.
    #therevore  checking -osm_id IN (id1,id2,id3) is super slow, but
    # osm_id IN (-id1,-id2,-id3) is fast. But it needs some more memory in python to
    # store the negatives copy as well
    if s.debug_xml :
        yield ET.Element('debug',{'status':'starting line query'})
    first=True
    for row_dict in dbutils.g_query_ids(s.c,query,g_negate(a.all_subtract('rels','done_ids')),'osm_id',step=250) :
        if first :
            start_t=time.time()
            #l.log('rels _line output start',start_t)
            first=False

        if a.is_in('done_ids',row_dict['id']) :
            continue
        #collapse hstore tags 
        tags=row_dict.pop('json_tags') if 'json_tags' in row_dict else {}
        if double_query_mode :
            cursor2.execute(query2+str(row_dict['id']))
            #add this missing data, ORDER important
            json_tags2,row_dict['members']=cursor2.fetchone()
            tags={**tags,**json_tags2}
        else :
            tags={**tags,**row_dict.pop('json_tags2')} if 'json_tags2' in row_dict else tags
        yield rel_to_xml(row_dict,tags,s.new_jsonb_schema)
        if s.debug_xml :
            yield ET.Element('debug',{'previous':str(row_dict['id']),
                'done_ids_len':str(a_len('done_ids')),'ids_len':str(len_ids),
                'table':table_name})
        add_done_ids(row_dict['id'])
        log.l.simplerate(a_len('done_ids'),'rels',len_ids)
    if first :
        #edgecase when query returned 0 items
        start_t=time.time()
    log.l.log('rels _line output end',(time.time()-start_t))

    table_name=tbl_rels
    log.l.log('reading table',table_name,'...')
    if s.new_jsonb_schema :
        query=f'SELECT id,members,tags AS json_tags FROM {table_name}'
    else :
        #in this table, tags is ::text[], not a hstore
        query=f'SELECT id,members,hstore_to_json(tags::hstore) AS json_tags FROM {table_name}'
    #bigger step than previous, because there is (heurisitcally) less data for these "light" relations,
    # which have no interesting tags regarding rendering making them worthy of a place in _polygon or _line
    if s.debug_xml :
        yield ET.Element('debug',{'status':'starting rels query'})
    for row_dict in dbutils.g_query_ids(s.c,query,a.all_subtract('rels','done_ids'),'id',step=300) :
        if a.is_in('done_ids',row_dict['id']) :
            continue
        #collapse hstore tags 
        tags=row_dict.pop('json_tags') if 'json_tags' in row_dict else {}
        yield rel_to_xml(row_dict,tags,s.new_jsonb_schema)
        if s.debug_xml :
            yield ET.Element('debug',{'previous':str(row_dict['id']),
                'done_ids_len':str(a_len('done_ids')),'ids_len':str(len_ids),
                'table':table_name})
        add_done_ids(row_dict['id'])
        log.l.simplerate(a_len('done_ids'),'rels',len_ids)
    log.l.finishrate()
    a.clear('rels')

def way_to_xml(row_dict:dict,tags:dict)->ET.Element :
    attrs,col_tags=split_tags_out(row_dict,('id','nodes'))
    # KEEP tags and row_dict separate:
    # https://www.openstreetmap.org/way/513097887 defines an id='1nh5Cbt9_EsnMhdH5T3hnPXQguY=' !!!

    way=ET.Element('way',{'id':str(attrs['id'])})
    if 'nodes' in attrs:
        for nd in attrs['nodes'] :
            ET.SubElement(way,'nd',{'ref':str(nd)})
    have_keys=set()
    for t in (tags,col_tags) :
        for k,v in t.items() :
            if k not in have_keys :
                ET.SubElement(way,'tag',{'k':str(k),'v':str(v)})
                have_keys.add(k)
    return way

def create_ways(s:settings.Settings,a:Accumulator)->typing.Iterator[ET.Element] :
    tbl_ways=s.tables['_ways']['name']
    table_name=s.tables['_polygon']['name']
    a_add=a.add
    a_len=a.len
    add_done_ids=lambda i:a_add('done_ids',i)
    a.clear('done_ids')
    len_ids=a_len('ways')

    log.l.log('reading table',table_name,'...')
    read_columns=[f'{table_name}.osm_id AS id',
        f'hstore_to_json({table_name}.tags) AS json_tags', #polygons stores a hstore even in the new_jsonb_schema
        f'{tbl_ways}.tags AS json_tags2' if s.new_jsonb_schema else f'hstore_to_json({tbl_ways}.tags::hstore) AS json_tags2',
        # do we need this ? no
        #f'{table_name}.way_area AS area',
        #I think real does not exist and real->float4
        *list(dbutils.get_columns_of_types(s.c,('int4','int','int8','int16','text','real','float4','float8'),table_name))
    ]
    query='SELECT '+(','.join(read_columns))
    query+=f',{tbl_ways}.nodes FROM {table_name} JOIN {tbl_ways}'
    query+=f' ON {table_name}.osm_id={tbl_ways}.id'

    for row_dict in dbutils.g_query_ids(s.c,query,a.all('ways'),'osm_id') :
        if a.is_in('done_ids',row_dict['id']) :
            continue
        #collapse hstore tags 
        tags=row_dict.pop('json_tags') if 'json_tags' in row_dict else {}
        tags={**tags,**row_dict.pop('json_tags2')} if 'json_tags2' in row_dict else tags
        yield way_to_xml(row_dict,tags)
        add_done_ids(row_dict['id'])
        log.l.simplerate(a_len('done_ids'),'ways',len_ids)

    #and now with _line
    table_name=s.tables['_line']['name']
    log.l.log('reading table',table_name,'...')
    read_columns=[f'{table_name}.osm_id AS id',
        f'hstore_to_json({table_name}.tags) AS json_tags',
        f'{tbl_ways}.tags AS json_tags2' if s.new_jsonb_schema else f'hstore_to_json({tbl_ways}.tags::hstore) AS json_tags2',
        *list(dbutils.get_columns_of_types(s.c,('int4','int','int8','int16','text','real','float4','float8'),table_name))
    ]
    query='SELECT '+(','.join(read_columns))
    query+=f',{tbl_ways}.nodes FROM {table_name} JOIN {tbl_ways}'
    query+=f' ON {table_name}.osm_id={tbl_ways}.id'

    for row_dict in dbutils.g_query_ids(s.c,query,a.all_subtract('ways','done_ids'),'osm_id') :
        if a.is_in('done_ids',row_dict['id']) :
            continue
        #collapse hstore tags 
        tags=row_dict.pop('json_tags') if 'json_tags' in row_dict else {}
        tags={**tags,**row_dict.pop('json_tags2')} if 'json_tags2' in row_dict else tags
        yield way_to_xml(row_dict,tags)
        add_done_ids(row_dict['id'])
        log.l.simplerate(a_len('done_ids'),'ways',len_ids)

    #everything else was queried, only _ways remains
    table_name=tbl_ways
    log.l.log('reading table',table_name,'...')
    #in this table, tags::text[], not yet a hstore
    if s.new_jsonb_schema :
        query=f'SELECT id,nodes,tags AS json_tags FROM {table_name}'
    else :
        query=f'SELECT id,nodes,hstore_to_json(tags::hstore) AS json_tags FROM {table_name}'
    for row_dict in dbutils.g_query_ids(s.c,query,a.all_subtract('ways','done_ids'),'id') :
        if a.is_in('done_ids',row_dict['id']) :
            continue
        #collapse hstore tags 
        tags=row_dict.pop('json_tags') if 'json_tags' in row_dict else {}
        yield way_to_xml(row_dict,tags)
        add_done_ids(row_dict['id'])
        log.l.simplerate(a_len('done_ids'),'ways',len_ids)
    log.l.finishrate()
    a.clear('ways')

def g_negate(g:typing.Iterator[int]) :
    for i in g :
        yield -i

def node_to_xml(row_dict:dict,tags:dict)->ET.Element :
    attrs,col_tags=split_tags_out(row_dict,('id','lat','lon'))
    attrs['id']=str(attrs['id'])
    node=ET.Element('node',attrs)
    have_keys=set()
    for t in (tags,col_tags) :
        for k,v in t.items() :
            if k not in have_keys :
                ET.SubElement(node,'tag',{'k':str(k),'v':str(v)})
                have_keys.add(k)
    return node

def split_tags_out(row_dict:dict,keep_keys:typing.Collection[str])->typing.Collection[dict] :
    ''' Given a row_dict, it also contains tags from the database columns.
    Separate the row_dict into row_dict with known keys, and tags with all
    other keys.
    '''
    dest_dict={}
    tags={}
    for k,v in row_dict.items() :
        if k in keep_keys :
            dest_dict[k]=v
        else :
            tags[k]=v
    return (dest_dict,tags,)

async def create_nodes(s:settings.Settings,a:Accumulator)->typing.Iterator[ET.Element] :
    table_name=s.tables['_point']['name']
    a.clear('done_ids')
    a_add=a.add
    a_len=a.len
    add_done_ids=lambda i:a_add('done_ids',i)
    len_ids=a_len('nodes')

    log.l.log('reading table',table_name,'...',clearline=True)
    read_columns=[f'{table_name}.osm_id AS id',
        f'hstore_to_json({table_name}.tags) AS json_tags',
        f'ST_X(ST_Transform({table_name}.way,4326)) AS lon',
        f'ST_Y(ST_Transform({table_name}.way,4326)) AS lat',
        *list(dbutils.get_columns_of_types(s.c,('int4','int','int8','int16','text'),table_name))
    ]
    query='SELECT '+(','.join(read_columns))+f' FROM {table_name}'

    for row_dict in dbutils.g_query_ids(s.c,query,a.all('nodes'),'osm_id') :
        if a.is_in('done_ids',row_dict['id']) :
            continue
        # extract the json_tags into tags
        tags=row_dict.pop('json_tags') if 'json_tags' in row_dict else {}
        # as a str, a number with 9999 end digits will waste space.
        # change all lat/lons :7.543702599999998->7.5437026.
        # 10 digit degrees is +- 0.011mm precision
        row_add={k:str(round(row_dict[k],10)) for k in ('lat','lon')}
        yield node_to_xml({**row_dict,**row_add},tags)
        add_done_ids(row_dict['id'])
        log.l.simplerate(a_len('done_ids'),'nodes',len_ids)
    log.l.finishrate()
    log.l.log('now querying flatnodes file for missing nodes')
    to_get_lat_lons=set()

    for batch in g_batches(a.all_subtract('nodes','done_ids'),5_000) :
        async for osm_id,lat,lon in dbutils.get_latlon_str_from_flatnodes(batch,s) :
            #osm_id,lat and lon are already strings (don't bother to convert+reconvert them)
            osm_id_int=int(osm_id)
            if a.is_in('done_ids',osm_id_int) :
                continue
            yield node_to_xml({'id':osm_id,'lat':lat,'lon':lon},{})
            add_done_ids(osm_id_int)
            log.l.simplerate(a_len('done_ids'),'nodes',len_ids)
    log.l.finishrate()
    a.clear('nodes')

def g_batches(generator:typing.Iterator,batch_size)->typing.Iterator[typing.Collection] :
    ''' Return sets of items yielded by generator of length at
    most batch_size. WARNING: types must be hashable, they are
    returned in sets (not lists) and are assumed to be unique.
    '''
    current_batch=set()
    for i in generator :
        current_batch.add(i)
        if len(current_batch)>=batch_size :
            yield current_batch
            current_batch=set()
    if len(current_batch)!=0 :
        yield current_batch



