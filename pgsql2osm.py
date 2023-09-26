#!/usr/bin/python3

#import xml.etree.ElementTree as ET
import lxml.etree as ET
# for streaming xml output, reduces ram footprint
import lxml.etree
import time
import psycopg2
import typing
import os
import json
import asyncio

import sys
import argparse

def regions_lookup(isocode:str) :
    isocode=isocode.upper().replace('_','-')
    with open(os.path.dirname(__file__)+'/regions.csv') as f:
        regions=f.read().strip().split('\n')
    headers=regions.pop(0).split(',')
    search_cols=[]
    for ix,h in enumerate(headers) :
        if h.find('iso')>=0 :
            search_cols.append(ix)
    for row in regions :
        r_d=row.split(',')
        for i in search_cols :
            if r_d[i].find(isocode)>=0 :
                if r_d[i]==isocode :
                    return r_d[headers.index('name')],r_d[headers.index('osm_id')]
    print('Error iso boundary not found:',isocode)
    exit(1)

class Logger() :
    def __init__(self) :
        self._ready=False
        self.previous_prependline=False
        self.previous_clearline=None

    def check_ready(self) :
        assert self._ready, 'Need to run .set_phases first'
    def set_phases(self,phases:typing.Collection[str]) :
        if self._ready :
            raise ValueError('Cannot .set_phases multiple times')
        self.phases=phases
        self.str_maxlen_phase=max(list(map(len,phases)))
        self.current_phase=0 #index into phases list
        self._ready=True

    def next_phase(self) :
        self.check_ready()
        self.current_phase+=1

    def log(self,*msg:typing.Any,clearline=False,prependline=False) :
        ''' Like print, accept a list of Any-typed items and print their .__str__()
        space-separated. Keeps track of the current phase.
        clearline==True will clear the line. Use when in a loop to display a counter going up
        prependline==True will not clear the line but leave the end without a newline.
            Use just before a .log(clearline=True) to add information before
        '''
        #self.check_ready() dont't for performance reasons
        assert int(clearline)+int(prependline)<2, 'not both clearline and prependline can be True'
        str_msg=' '.join(map(str,msg))
        l=self.str_maxlen_phase+5
        phase=str(self.current_phase+1)+'/'
        phase+=str(len(self.phases))
        phase+=' '+self.phases[self.current_phase]
        # a clearline will trigger clearing the line EXCEPT when the previous log was a prependline
        # -- OR --
        # two prependlines after eachother will trigger clearing the line: the newer erases the older
        if ((prependline or clearline) and not self.previous_prependline) or (self.previous_prependline and prependline):
            print(end='\033[2K\r',file=sys.stderr)
        if not (clearline or prependline) and self.previous_clearline :
            print(file=sys.stderr) #reset clearline
        #a bit crazy syntax, but I want the field length to be variably dependent on self.str_maxlen_phase
        if not self.previous_prependline :
            # f-string will make -> '{:<13}'
            # str.format will do -> 'one          '
            str_msg=f'[ {{:<{l}}}] {{}}'.format(phase,str_msg)
        print(str_msg,end=('' if clearline else ' ' if prependline else '\n'),file=sys.stderr)
        self.previous_prependline=prependline
        self.previous_clearline=clearline

def percent(numer:int,denom:int)->str :
    ''' Return the str(float(numer/denom)*100) with 3 sigfigs,
    '''
    r=numer/denom*100
    # ljust for 3.0 -> 3.00
    if r<1.0 :
        return str(round(r,3)).ljust(5,'0')+'%'
    elif r<10.0 :
        return str(round(r,2)).ljust(4,'0')+'%'
    else :
        return str(round(r,1)).ljust(3,'0')+'%'

async def chain(*generators:typing.Iterator)->typing.Iterator:
    for g in generators :
        try :
            #async does not like 'yield from' syntax, but this works
            for i in g :
                yield i
        #is an asyncio generator
        except TypeError :
            async for i in g :
                yield i

async def get_latlon_str_from_flatnodes(osm_ids:typing.Collection[int])->typing.Iterator :
    #beware, need to exchange lonlat -> latlon
    osm_ids=list(osm_ids) #for the range-indexing
    a=await asyncio.create_subprocess_exec('/home/user/src/osm2pgsql/build/get_lonlat',
        '/mnt/dbp/maps/planet.bin.nodes',
        stdout=asyncio.subprocess.PIPE,stdin=asyncio.subprocess.PIPE)
    # some osm_ids may error out. in that case get_lonlat just ignores them. therefore, take the output
    # osm_id as source of truth.
    a.stdin.write((' '.join(map(str,osm_ids))).encode())
    a.stdin.close()
    #read all results
    while (line:=(await a.stdout.readline()).strip().decode()) :
        #l.log('read line',line)
        x,y,osm_id=line.split(';')
        yield (osm_id,y,x)

def all_nwr_within(c:psycopg2.extensions.cursor,accumulator:dict,args:argparse.Namespace) :
    from_rel_id=False
    if args.bounds_geojson!=None :
        with open(args.bounds_geojson,'r') as f :
            geojson=f.read().strip()
        way_constr=f"ST_GeomFromGeoJSON('{geojson}'::jsonb)"
        way_constr_a=f'way&&ST_Transform({way_constr},3857)'
        way_constr_b=f'ST_Intersects(way,ST_Transform({way_constr},3857))'
    elif args.bounds_rel_id!=None :
        osm_rel_id=args.bounds_rel_id
        from_rel_id=True
    elif args.bounds_iso!=None :
        c_name,osm_rel_id=regions_lookup(args.bounds_iso)
        l.log(f"Suggested output filename: '{c_name}.osm'")
        osm_rel_id=int(osm_rel_id)
        from_rel_id=True
    elif args.bounds_box!=None :
        lon_from,lat_from,lon_to,lat_to=tuple(map(float,args.bounds_box.split(',')))
        way_constr_a=f'way&&ST_Transform(ST_MakeEnvelope({lon_from}, {lat_from}, {lon_to}, {lat_to}, 4326),3857)'
        way_constr_b=f'ST_Intersects(way,ST_Transform(ST_MakeEnvelope({lon_from}, {lat_from}, {lon_to}, {lat_to}, 4326),3857))'
    else :
        l.log('Error: no boundary provided.')
        l.log("If you are sure to export the whole planet, use --bbox='-180,-89.99,180,89.99'")
        exit(1)
    if from_rel_id :
        #relation ids are stored negative
        way_constr_a=f'way&&(SELECT relbound.way FROM planet_osm_polygon AS relbound WHERE osm_id={-osm_rel_id})'
        way_constr_b=f'ST_Intersects(way,(SELECT relbound.way FROM planet_osm_polygon AS relbound WHERE osm_id={-osm_rel_id}))'
        #way_constr=f'way && ST_Transform(ST_MakeEnvelope({lon_from}, {lat_from}, {lon_to}, {lat_to}, 4326),3857)'
# 1a) select all nodes WHERE way ST_Within(bbox);
    l.log('executing big query on planet_osm_point...',clearline=True)
    c.execute(f'SELECT osm_id FROM planet_osm_point WHERE {way_constr_b};')
    for row in g_from_cursor(c,verbose=True,prefix_msg='planet_osm_point ') :
        accumulator['nodes'].add(row['osm_id'])
    l.log(len(accumulator['nodes']),'nodes within bounds')

    # 1b) select all ways,rels FROM planet_osm_polygon WHERE way ST_Within(bbox);
    l.log('executing big query on planet_osm_polygon...',clearline=True)
    c.execute(f'SELECT osm_id FROM planet_osm_polygon WHERE {way_constr_a};')
    for row in g_from_cursor(c,verbose=True,prefix_msg='planet_osm_polygon ') :
        id=row['osm_id']
        if id>0 :
            accumulator['ways'].add(id)
        else :
            accumulator['rels'].add(-id)
    # 1c) select all ways,rels FROM planet_osm_line WHERE way ST_Within(bbox);
    # planet_osm_roads is not needed in that fashion, because it is a strict subset
    # of planet_osm_line, and we're only collecting ids of objects for now
    l.log(len(accumulator['ways']),'ways,',len(accumulator['rels']),'rels from planet_osm_polygon')
    l.log('executing big query on planet_osm_line...',clearline=True)
    c.execute(f'SELECT osm_id FROM planet_osm_line WHERE {way_constr_b};')
    for row in g_from_cursor(c,verbose=True,prefix_msg='planet_osm_line ') :
        id=row['osm_id']
        if id>0 :
            accumulator['ways'].add(id)
        else :
            accumulator['rels'].add(-id)
    l.log(len(accumulator['ways']),'ways within bounds')
    l.log(len(accumulator['rels']),'rels within bounds')

def g_adaptive_parent_multiquery(c:psycopg2.extensions.cursor,ids:list,
        queries:typing.Collection[str],
        nodelist_lambda_tuples:typing.Collection[typing.Collection[typing.Callable]]
        )->typing.Iterator :
    ''' The database has some indexes on the bigint[] columns that contain
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
    total_processed_nodes=0
    start_chunk_size=50 #const
    chunk_size=start_chunk_size
    avg_accepted_chunk_size=0
    chunks_unchanged_chunk_size=0
    stable=False
    chunks=0
    c.execute("SET statement_timeout='1s';")
    printed_slow_warning=False
    while total_processed_nodes<len(ids) :
        start_time=time.time()
        nodes_chunk=ids[total_processed_nodes:total_processed_nodes+chunk_size]
        try :
            results=[None for q in queries]
            for ix,q in enumerate(queries) :
                #apply callbacks
                data_s=list(map(lambda f:f(nodes_chunk),nodelist_lambda_tuples[ix]))
                q_uery=q.format(*data_s)
                #l.log(q_uery)
                c.execute(q_uery)
                results[ix]=list(g_from_cursor(c))
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
                    pid=os.getpid()
                    l.log('WARNING: queries are running very slowly, the index may not exist.')
                    l.log(f'\tplease kill this process: "kill {pid}" and create indexes:')
                    l.log('\tCREATE INDEX planet_osm_ways_nodes_bucket_idx ON planet_osm_ways')
                    l.log('\t\tUSING GIN (planet_osm_index_bucket(nodes))')
                    l.log('\t\tWITH (fastupdate = off);')
                    printed_slow_warning=True
                #see below, db forgets it. but don't rely on its forgetfulness
                c.execute("SET statement_timeout=0;")
            else :
                #it forgets that at each new transaction
                c.execute("SET statement_timeout='1s';")
    #set it back, maybe run an ABORT; instead ?
    c.execute("SET statement_timeout='2h';")

    
def nodes_parent_wr(c:psycopg2.extensions.cursor,accumulator:dict) :
    # 2a) foreach node_id :
    l.log('checking parent ways of',len(accumulator['nodes']),'nodes')
    way_count=0
    rel_count=0
    node_count=0
    #for node_id_chunk in g_batches(accumulator['nodes'],11) :
    #    # 2b) select all ways WHERE ARRAY[node_id]::bigint[] <@ nodes;
    #    node_ids=','.join(map(str,node_id_chunk))
    #    c.execute(f'SELECT id FROM planet_osm_ways WHERE ARRAY[{node_ids}]::bigint[] && nodes;')
    #    node_count+=len(node_id_chunk)
    #    for row in g_from_cursor(c) :
    #        # SAVE ALL ways ids
    #        way_count+=1
    #        accumulator['ways'].add(row['id'])

    #    # 2c) select all rels WHERE ARRAY[node_id]::bigint[] <@ parts;
    #    #assert that the id in question is a node <=> n{id} exists in members (AND NOT w{id})
    #    node_member_ids=','.join(map(lambda i:f"'n{i}'",node_id_chunk))
    #    c.execute(f'''SELECT id FROM
    #        (SELECT id,members FROM planet_osm_rels
    #            WHERE ARRAY[{node_ids}]::bigint[] && parts) AS parts_indexed
    #        WHERE ARRAY[{node_member_ids}] && members;''')
    #    for row in g_from_cursor(c) :
    #        # SAVE ALL rel ids
    #        accumulator['rels'].add(row['id'])
    #        rel_count+=1
    #    print(end='\r\033[2K')
    #    print(way_count,'ways,',rel_count,
    #        'rels parents of node',node_count,'/',
    #        len(accumulator['nodes']),end='\r')

    try :
        c.execute('SELECT planet_osm_index_bucket(ARRAY[]::bigint[])')
        use_bucket_func=True
    except psycopg2.errors.UndefinedFunction :
        use_bucket_func=False #does not exist
    if use_bucket_func :
        add_buck='planet_osm_index_bucket(ARRAY[{0}]::bigint[]) && planet_osm_index_bucket(nodes) AND '
    else :
        add_buck=''

    if new_jsonb_schema :
        # the ::cahr(1) cast IS IMPORTANT for index performance, 10x or more slower without it
        rels_query="SELECT id FROM planet_osm_rels WHERE planet_osm_member_ids(members,'N'::char(1)) && ARRAY[{0}]::bigint[];"
        rels_lambdas=(lambda i:','.join(map(str,i)),)
    else :
        parts_indexed="SELECT id,members FROM planet_osm_rels WHERE ARRAY[{0}]::bigint[] && parts"
        members_where="ARRAY[{1}] && members"
        rels_query=f'SELECT id FROM ({parts_indexed}) AS parts_indexed WHERE {members_where};'
        rels_lambdas=(lambda i:','.join(map(str,i)),lambda i:','.join(map(lambda j:f"'n{j}'",i)),)
    
    for node_c,way_ids,rel_ids in g_adaptive_parent_multiquery(c,
            list(accumulator['nodes']),
            ('SELECT id FROM planet_osm_ways WHERE '+add_buck+'ARRAY[{0}]::bigint[] && nodes;',
                rels_query),
            [(lambda i:','.join(map(str,i)),),rels_lambdas]) :
        node_count+=node_c
        if node_count%8==0 :
            l.log(way_count,'ways,',rel_count,
                'rels parents of node',node_count,'/',len(accumulator['nodes']),
                '    ',percent(node_count,len(accumulator['nodes'])),clearline=True)
        for way in way_ids:
            way_count+=1
            accumulator['ways'].add(way['id'])
        for rel in rel_ids:
            rel_count+=1
            accumulator['rels'].add(rel['id'])
    l.log(len(accumulator['ways']),'ways forward from nodes')
    l.log(len(accumulator['rels']),'rels forward from nodes')

def ways_parent_r(c:psycopg2.extensions.cursor,accumulator:dict) :
    # 3a) foreach way_id :
    #for way_id in accumulator['ways'] :
    #    # 3b) select all rels WHERE ARRAY[way_id]::bigint[] <@ parts;
    #    #assert that the id in question is a way <=> w{id} exists in members (AND NOT n{id})
    #    c.execute(f'''SELECT id FROM
    #        (SELECT id,members FROM planet_osm_rels
    #            WHERE ARRAY[{way_id}]::bigint[]<@parts) AS parts_indexed
    #        WHERE ARRAY['w{way_id}']<@members;''')
    #    for row in g_from_cursor(c) :
    #        # SAVE ALL rel ids
    #        accumulator['rels'].add(row['id'])
    way_count=0
    rel_count=0
    for way_c,rel_ids in g_adaptive_parent_multiquery(c,list(accumulator['ways']),
            ('''SELECT id FROM
            (SELECT id,members FROM planet_osm_rels
                WHERE ARRAY[{0}]::bigint[] && parts) AS parts_indexed
            WHERE ARRAY[{1}] && members;''',),[(lambda i:','.join(map(str,i)),
            lambda i:','.join(map(lambda j:f"'w{j}'",i)),)]) :
        way_count+=way_c
        for rel in rel_ids:
            rel_count+=1
            accumulator['rels'].add(rel['id'])
        if way_count%8==0 :
            l.log(rel_count,'rels parents of way',way_count,'/',len(accumulator['ways']),
                '    ',percent(way_count,len(accumulator['ways'])),clearline=True)
    l.log(len(accumulator['rels']),'rels forward')

def rels_children_nwr(c:psycopg2.extensions.cursor,accumulator:dict,only_multipolygon_rels=False,without_rels=False) :
    # 4) BACKpropagation: resolve to take in all rels->ways->nodes
    # 4a) foreach rel_id: add all its members'ids as n{id} -> node, w{id} -> way, or r{id} -> rel
    node_count=0
    way_count=0
    rel_count=0
    tot_count=0
    #relation can be of very different types. a multipolygon could be a forest for example,
    # with an associated geometry. constrast that to a type="route" or type="superroute",
    # thore are only groups of already representable ways on the map
    # see osm wiki /Types_of_relation for more
    if new_jsonb_schema :
        multipolygon_constr=" AND (tags->>'type')='multipolygon'"
    else :
        multipolygon_constr=" AND ((tags::hstore)->'type')='multipolygon'"
    multipolygon_constr=multipolygon_constr if only_multipolygon_rels else ''

    for j in (1,2): #repeat twice to resolve rels that have rels as children
        buffer_add_rels=set()
        for rel_id in accumulator['rels'] :
            #for row in g_query_ids()
            tot_count+=1
            c.execute(f'SELECT members FROM planet_osm_rels WHERE id={rel_id}{multipolygon_constr};')
            for row in g_from_cursor(c) :
                ms=row['members']
                if new_jsonb_schema :
                    for m in ms :
                        if m['type']=='N' :
                            accumulator['nodes'].add(m['ref'])
                            node_count+=1
                        elif m['type']=='W' :
                            accumulator['ways'].add(m['ref'])
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
                            accumulator['nodes'].add(osm_id)
                            node_count+=1
                        elif osm_type=='w' :
                            accumulator['ways'].add(osm_id)
                            way_count+=1
                        elif osm_type=='r' :
                            buffer_add_rels.add(osm_id)
                            if not without_rels :
                                rel_count+=1
                        else :
                            raise ValueError(f'''Encountered invalid members[]
                                element of rel id {rel_id} in members={repr(ms)}
                                at index [{i}]={repr(ms[i])}''')
            l.log(node_count,'nodes,',way_count,'ways,',rel_count,'rels children of rel',tot_count,'/',
                len(accumulator['rels']),'    ',percent(tot_count,len(accumulator['rels'])),clearline=True)
        if without_rels :
            l.log('collected',node_count,'nodes,',way_count,'ways,',rel_count,'rels',
                'from',len(accumulator['rels']),'rels children')
            return #after first run
        for rel_id in buffer_add_rels :
            accumulator['rels'].add(rel_id)
    l.log('collected',node_count,'nodes,',way_count,'ways,',rel_count,'rels',
        'children of',len(accumulator['rels']),'rels')

def ways_children_n(c:psycopg2.extensions.cursor,accumulator:dict) :
    # 4b) foreach way_id: add all its nodes[] ids
    way_count=0
    node_count=0
    for way_id in accumulator['ways'] :
        way_count+=1
        c.execute(f'SELECT nodes FROM planet_osm_ways WHERE id={way_id};')
        for row in g_from_cursor(c) :
            for i in row['nodes'] :
                accumulator['nodes'].add(i)
                node_count+=1
        if way_count%32==0 :
            l.log(node_count,'nodes children of way',way_count,'/',len(accumulator['ways']),
                '    ',percent(way_count,len(accumulator['ways'])),clearline=True)
    l.log('collected',node_count,'nodes,','children of',len(accumulator['ways']),'ways')


async def stream_osm_xml(c:psycopg2.extensions.cursor,args:argparse.Namespace) :
    ''' Query osm2pgsql-imported postgres database for nodes, ways and rels and stream
    an xml representation of them into args.out_file. Attempts to select objects that are in
    the given bounds. But the dependencies are sometimes required, so more data that just
    within the bounds will be included. Currently, no geometric features are clipped in
    any way.
    The args.bounds can describe a multitude of formats:
        * <rel_id_integer> will fetch the bounding box from database
        * <filename_of_geojson> will read the geojson description
        * <lon_from>,<lat_from>,<lon_to>,<lat_to> will make a rectangle.
    '''
    with_parents=False
    phases=['within','parents','children','write']
    if not with_parents :
        phases.remove('parents')
    l.set_phases(phases)
    #SELECT workflow to get all element [ids ONLY] in bounding box or boundary:
    accumulator={'nodes':set(),'ways':set(),'rels':set()}

    all_nwr_within(c,accumulator,args)
    if with_parents :
        l.next_phase() #parents
        nodes_parent_wr(c,accumulator)
        #ways_parent_r(c,accumulator)

    l.next_phase() #children
    # do we want big forests ? yes even outside the bounds
    rels_children_nwr(c,accumulator,only_multipolygon_rels=True,without_rels=True)
    ways_children_n(c,accumulator)

    counts=[len(accumulator[i])for i in ('nodes','ways','rels')]
    l.log('dumping',counts[0],'nodes,',counts[1],'ways,',counts[2],'rels in total')

    # ONLY after all ids have been resolved, do we actually query the data, RAM-inefficient otherwise
    # more RAM-inefficient for bigger extracts. do more of a streaming from database to file approach
    l.next_phase() #query
    out_file=sys.stdout.buffer if args.out_file=='-' else args.out_file
    with lxml.etree.xmlfile(out_file,encoding='utf-8') as xml_out :
        xml_out.write_declaration()
        with xml_out.element('osm',{
                'version':'0.6',
                'generator':time.strftime('psql2osm.py %F')
        }) :
            async for el in chain(
                    create_nodes(c,list(accumulator['nodes'])),
                    create_ways(c,list(accumulator['ways'])),
                    create_relations(c,list(accumulator['rels'])),
            ) :
                xml_out.write(el)

def rel_to_xml(row_dict:dict)->ET.Element :
    rel=ET.Element('relation',{'id':str(row_dict['id'])})
    #collapse hstore tags
    try :
        for k,v in row_dict.pop('json_tags').items() :
            row_dict[k]=v
        for k,v in row_dict.pop('json_tags2').items() :
            row_dict[k]=v
    except KeyError :
        pass
    try :
        if new_jsonb_schema :
            for m in row_dict.pop('members') :
                trsl={'N':'node','W':'way','R':'relation'}
                ET.SubElement(rel,'member',{'type':trsl[m['type']],'ref':str(m['ref']),'role':m['role']})
        else :
            m=iter(row_dict.pop('members'))
            while True :
                try :
                    mixed_id=next(m)
                    role=next(m)
                    osm_type={'n':'node','w':'way','r':'relation'}[mixed_id[0]]
                    id=mixed_id[1:]
                    ET.SubElement(rel,'member',{'type':osm_type,'ref':str(id),'role':role})
                except StopIteration :
                    break
    except KeyError :
        pass
    for k,v in row_dict.items() :
        if k not in ('nodes','id') :
            # make sure a float like v=18.572e6 does not have the 'e' in str() -> YES: "18572000.0"
            ET.SubElement(rel,'tag',{'k':str(k),'v':str(v)})
    return rel

def create_relations(c:psycopg2.extensions.cursor,ids:list)->typing.Iterator[ET.Element] :
    table_name='planet_osm_polygon'
    l.log(0,'/',len(ids),'rels, reading table',table_name,'...',clearline=True)
    read_columns=[f'-{table_name}.osm_id AS id',
        f'hstore_to_json({table_name}.tags) AS json_tags',
        # we need to merge the _polygon tags with the _rels tags.
        # _planet tags are overwritten by _rels tags because
        # json_tags is first loaded, the json_tags2 overloads all existing and non-existing keys
        'planet_osm_rels.tags AS json_tags2' if new_jsonb_schema else 'hstore_to_json(planet_osm_rels.tags::hstore) AS json_tags2',
        # do we need this ? no ; especially the tags->'area'='yes' will overwrite 'area' if it exists
        #f'{table_name}.way_area AS area',
        #I think real does not exist and real->float4
        *list(get_columns_of_types(c,('int4','int','int8','int16','text','real','float4','float8'),table_name))
    ]
    query='SELECT '+(','.join(read_columns))
    query+=f',planet_osm_rels.members FROM {table_name} JOIN planet_osm_rels'
    query+=f' ON -{table_name}.osm_id=planet_osm_rels.id'

    # the JOIN makes this query incredibly slow...
    #query+=f' FROM {table_name} '

    done_ids=set()
    # psql does not have an index on -osm_id and does not understand *=-1 is bijective.
    #therevore  checking -osm_id IN (id1,id2,id3) is super slow, but
    # osm_id IN (-id1,-id2,-id3) is fast. But it needs some more memory in python to
    # store the negatives copy as well
    #for row_dict in g_query_ids(c,query,list(-i for i in ids),'osm_id',verbose=True,step=2) :
    for row_dict in g_query_ids(c,query,list(-i for i in ids),'osm_id',step=250) :
        #other_cursor.execute(f'SELECT members,hstore_to_json(tags::hstore) AS json_tags FROM planet_osm_rels WHERE id={row_dict["id"]}')
        ##l.log('querying',row_dict['id'])
        #columns=[i.name for i in other_cursor.description]
        #other_row=other_cursor.fetchone()
        #other_row_dict={k:v for k,v in zip(columns,other_row) if v!=None}
        #row_dict['members']=other_row_dict['members']
        #row_dict['json_tags2']=other_row_dict['json_tags']
        yield rel_to_xml(row_dict)
        done_ids.add(row_dict['id'])
        l.log(len(done_ids),'/',len(ids),'rels','    ',percent(len(done_ids),len(ids)),clearline=True)
    missing_ids=set()
    for i in ids :
        if i not in done_ids :
            missing_ids.add(i)

    table_name='planet_osm_rels'
    l.log(len(done_ids),'/',len(ids),prependline=True)
    l.log('rels: still',len(missing_ids),'missing, reading table',table_name)
    #in this table, tags is ::text[], not a hstore
    if new_jsonb_schema :
        query=f'SELECT id,members,tags AS json_tags FROM {table_name}'
    else :
        query=f'SELECT id,members,hstore_to_json(tags::hstore) AS json_tags FROM {table_name}'
    count=len(done_ids)+0
    #bigger step than previous, because there is (heurisitcally) less data for these "light" relations
    for row_dict in g_query_ids(c,query,list(missing_ids),'id',step=300) :
        yield rel_to_xml(row_dict)
        count+=1
        l.log(count,'/',len(ids),'rels','    ',percent(count,len(ids)),clearline=True)

def way_to_xml(row_dict:dict)->ET.Element :
    #collapse hstore tags 
    try :
        for k,v in row_dict.pop('json_tags').items() :
            row_dict[k]=v
        for k,v in row_dict.pop('json_tags2').items() :
            row_dict[k]=v
    except KeyError :
        pass
    way=ET.Element('way',{'id':str(row_dict['id'])})
    try :
        for nd in row_dict['nodes'] :
            ET.SubElement(way,'nd',{'ref':str(nd)})
    except KeyError :
        pass
    for k,v in row_dict.items() :
        if k not in ('nodes','id') :
            ET.SubElement(way,'tag',{'k':str(k),'v':str(v)})
    return way

def create_ways(c:psycopg2.extensions.cursor,ids:list)->typing.Iterator[ET.Element] :
    table_name='planet_osm_polygon'
    l.log(0,'/',len(ids),'ways, reading table',table_name,'...',clearline=True)
    read_columns=[f'{table_name}.osm_id AS id',
        f'hstore_to_json({table_name}.tags) AS json_tags', #polygons stores a hstore even in the new_jsonb_schema
        'planet_osm_ways.tags AS json_tags2' if new_jsonb_schema else f'hstore_to_json(planet_osm_ways.tags::hstore) AS json_tags2',
        # do we need this ? no
        #f'{table_name}.way_area AS area',
        #I think real does not exist and real->float4
        *list(get_columns_of_types(c,('int4','int','int8','int16','text','real','float4','float8'),table_name))
    ]
    query='SELECT '+(','.join(read_columns))
    query+=f',planet_osm_ways.nodes FROM {table_name} JOIN planet_osm_ways'
    query+=f' ON {table_name}.osm_id=planet_osm_ways.id'

    done_ids=set()
    for row_dict in g_query_ids(c,query,ids,'osm_id') :
        yield way_to_xml(row_dict)
        done_ids.add(row_dict['id'])
        l.log(len(done_ids),'/',len(ids),'ways','    ',percent(len(done_ids),len(ids)),clearline=True)
    missing_ids=set()
    for i in ids :
        if i not in done_ids :
            missing_ids.add(i)

    table_name='planet_osm_ways'
    l.log(len(done_ids),'/',len(ids),prependline=True)
    l.log('ways: still',len(missing_ids),'missing, reading table',table_name)
    #in this table, tags::text[], not yet a hstore
    if new_jsonb_schema :
        query=f'SELECT id,nodes,tags AS json_tags FROM {table_name}'
    else :
        query=f'SELECT id,nodes,hstore_to_json(tags::hstore) AS json_tags FROM {table_name}'
    count=len(done_ids)+0
    for row_dict in g_query_ids(c,query,list(missing_ids),'id') :
        yield way_to_xml(row_dict)
        count+=1
        l.log(count,'/',len(ids),'ways','    ',percent(count,len(ids)),clearline=True)


def g_from_cursor(c:psycopg2.extensions.cursor,verbose=False,prefix_msg='')->typing.Iterator[dict]:
    ''' Assuming the query has already c.execute()d, return its results
    as a dict-generator'''
    columns=[i.name for i in c.description]
    tot_count=c.rowcount
    count=1 # for human display
    while (row:=c.fetchone())!=None :
        if verbose and count%32==0:
            l.log(prefix_msg+'row',count,'/',tot_count,'    ',percent(count,tot_count),clearline=True)
        yield {k:v for k,v in zip(columns,row) if v!=None}
        count+=1

def g_query_ids(c:psycopg2.extensions.cursor,query:str,
        ids:list,id_col:str,step=1000,verbose=False)->typing.Iterator[dict] :
    ''' Given an SQL query without the ending semicolon and where the last
    clause is a WHERE, append AND {id_col} IN (*ids) and yield those results.
    A psql syntax error will be thrown if ORDER BY, LIMIT are the last clause.
    '''
    l_ids=len(ids)
    init_query=query
    # case of 'SELECT ... FROM table' -> 'SELECT .. FROM table WHERE {append_query}'
    if init_query.find('WHERE')<0 :
        init_query+=' WHERE'
    for i in range(0,l_ids,step) :
        query=f'{init_query} {id_col} IN ('
        query+=','.join(map(str,ids[i:i+step]))
        query+=');'
        if verbose :
            l.log(query)
        c.execute(query)
        if verbose :
            l.log('query returned',c.rowcount,'rows')
        yield from g_from_cursor(c)

def get_columns_of_types(c:psycopg2.extensions.cursor,
        col_types:typing.Collection[str],table_name:str)->typing.Iterator[str] :
    values_col_types=",".join(["'"+i+"'" for i in col_types])
    c.execute(f'''
    SELECT colname,strtype FROM 
        (SELECT attname AS colname,
            (SELECT typname FROM pg_type WHERE oid=atttypid) AS strtype
        FROM pg_attribute WHERE attrelid=
            (SELECT oid FROM pg_class WHERE relname='{table_name}')
        ) AS columns
    WHERE strtype IN ({values_col_types})
        AND colname NOT IN ('osm_id');''')
    for colname,strtype in c.fetchall() :
        yield table_name+'.'+('"'+colname+'"' if colname.find(':')>0 else colname)

async def create_nodes(c:psycopg2.extensions.cursor,ids:list)->typing.Iterator[ET.Element] :
    table_name='planet_osm_point'
    l.log(0,'/',len(ids),'nodes, reading table',table_name,'...',clearline=True)
    read_columns=[f'{table_name}.osm_id AS id',
        f'hstore_to_json({table_name}.tags) AS json_tags',
        f'ST_X(ST_Transform({table_name}.way,4326)) AS lon',
        f'ST_Y(ST_Transform({table_name}.way,4326)) AS lat',
        *list(get_columns_of_types(c,('int4','int','int8','int16','text'),table_name))
    ]
    query='SELECT '+(','.join(read_columns))+f' FROM {table_name}'
    done_nodes=set()
    for row_dict in g_query_ids(c,query,ids,'osm_id') :
        row_attrs={k:str(row_dict[k]) for k in ('id','lat','lon')}
        # expand the json_tags to the tags and remove 'json_tags'
        try :
            for k,v in row_dict.pop('json_tags').items() :
                row_dict[k]=v
        except KeyError :
            pass
        node=ET.Element('node',row_attrs)
        for k,v in row_dict.items() :
            if k not in row_attrs :
                ET.SubElement(node,'tag',{'k':str(k),'v':str(v)})
        done_nodes.add(row_dict['id'])
        yield node
        if len(done_nodes)%32==0 :
            l.log(len(done_nodes),'/',len(ids),'nodes','    ',
                percent(len(done_nodes),len(ids)),clearline=True)
    l.log('now querying flatnodes file for missing nodes')
    to_get_lat_lons=set()
    #def g_in() :
    #    for node_id in ids :
    #        if node_id not in done_nodes :
    #            yield node_id
    count=len(done_nodes)+0
    for batch in g_batches((i for i in ids if i not in done_nodes),5_000) :
        #async with get_latlon_str_from_flatnodes(batch) as simple_nodes :
        #print('get_lonlat finished',len(ids)-len(done_nodes))
        #async for osm_id,lat,lon in simple_nodes :
        async for osm_id,lat,lon in get_latlon_str_from_flatnodes(batch) :
            yield ET.Element('node',{'id':str(osm_id),'lat':lat,'lon':lon})
            count+=1
            if count%16==0 :
                l.log(count,'/',len(ids),'nodes','    ',percent(count,len(ids)),clearline=True)
    print(file=sys.stderr)

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

l=Logger() #global variable

if __name__=='__main__' :

    parser=argparse.ArgumentParser(prog='pgsql2osm')

    parser.add_argument('get_lonlat_binary',
        help="Path to the get_lonlat binary")
    parser.add_argument('nodes_file',
        help='Path to the nodes file created by osm2pgsql at import')
    parser.add_argument('-d','--dsn',
        dest='postgres_dsn',
        help="The connection string to pass to psycopg2 eg 'host=localhost dbname=gis' port=5432",
        default='dbname=gis')
    #one of the following:
    parser.add_argument('-b','--bbox',
        dest='bounds_box',
        default=None,
        type=str,
        help='Rectangle boundary in the format lon_from,lat_from,lon_to,lat_to')
    parser.add_argument('-r','--osm-rel-id',
        dest='bounds_rel_id',
        default=None,
        help='Integer for the osm relation that should make the boundary',
        type=int)
    parser.add_argument('-i','--iso',
        dest='bounds_iso',
        default=None,
        help='Country or region code for looking up in regions.csv, to determine boundary')
    parser.add_argument('-g','--geojson',
        dest='bounds_geojson',
        default=None,
        help='Geojson file for determining the boundary')

    parser.add_argument('-o','--output',
        dest='out_file',
        help="Path where the output .osm should be written to. When '-', write to stdout",
        required=True)


    args=parser.parse_args()
    access=psycopg2.connect(args.postgres_dsn)
    other_cursor=access.cursor()
    j_schema=list(get_columns_of_types(other_cursor,('jsonb',),'planet_osm_rels'))
    t_schema=list(get_columns_of_types(other_cursor,('_text',),'planet_osm_rels'))
    new_jsonb_schema=len(j_schema)==2
    if new_jsonb_schema :
        assert len(t_schema)==0, 'Could not decide which middle db schema is used'
    else :
        assert len(t_schema)==2, 'Could not decide which middle db schema is used'

    print('INFO:','detected middle database layout:','new jsonb' if new_jsonb_schema else 'legacy text[]',file=sys.stderr)
    print('[ start ]',time.strftime('%F_%T'),file=sys.stderr)
    t=asyncio.run(stream_osm_xml(access.cursor(),args))
    #ET.ElementTree(t).write(sys.argv[3],encoding='utf-8',xml_declaration=True)
