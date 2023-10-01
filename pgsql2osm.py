#!/usr/bin/python3

import lxml.etree as ET
import time
import psycopg2
import typing
import os
import json
import asyncio

import sys
import argparse

##TODO

# - add rate measaurement, group log and percent messages to logger class.
#   -> add number repr with K, M and G
#   -> adaptive to prevent stdout slowdown: only print every 0.1s, only take time
#      every <count> loops, where count adapts to target every 0.05s only
#       x / total nodes (r{K}/s) p %
#       or a nodes, b ways children from x / total rels (r{K}/s) p %
#       and then the displayed rate is average only over last ~10s of samples
#   -> add "print_summary_last_line" for showing the 100%...

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

class DictAccumulator(Accumulator) :
    def __init__(self,named_data) :
        self.named_data=named_data
        self.data={k:set() for k in self.named_data}

    def add(self,k,i) :
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
    print('Error iso boundary not found:',isocode,file=sys.stderr)
    exit(1)

class Settings :
    def __init__(self,args:argparse.Namespace) :
        self.debug=args.debug
        self.debug_xml=False

        self.bounds_geojson=args.bounds_geojson
        self.bounds_rel_id=args.bounds_rel_id
        self.bounds_iso=args.bounds_iso
        self.bounds_box=args.bounds_box

        self.get_lonlat_binary=args.get_lonlat_binary
        self.nodes_file=args.nodes_file

        self.out_file=args.out_file
        
        self.access=psycopg2.connect(args.postgres_dsn)
        #use one cursor for everything
        self.c=self.access.cursor()
        self.c.execute('''SELECT f_table_name AS name,f_table_schema AS schema,
                f_geometry_column AS geom,srid,
                pg_table_size(f_table_name::text) AS size
            FROM geometry_columns WHERE srid!=0''')
        autodetect_tables=list(g_from_cursor(self.c))
        self.c.execute('''SELECT tablename AS name,schemaname AS schema,
                pg_table_size(tablename::text) AS size
            FROM pg_tables WHERE schemaname NOT IN ('information_schema','pg_catalog')''')
        autodetect_tables.extend(list(g_from_cursor(self.c)))
        self.tables={}
        geom_name_endings=('_point','_line','_polygon')
        nongeom_name_endings=('_ways','_rels')
        for row_dict in autodetect_tables :
            name=row_dict['name']
            n_end=None
            test_endings=geom_name_endings if 'geom' in row_dict else nongeom_name_endings
            for test_name_end in test_endings :
                if name.endswith(test_name_end) :
                    n_end=test_name_end
            if n_end==None :
                continue
            name=row_dict['schema']+'.'+row_dict['name']
            if n_end in self.tables and self.tables[n_end]['size']>row_dict['size']:
                continue #take the biggest size table
            self.tables[n_end]={'name':name,'size':row_dict['size']}
            if 'geom' in row_dict :
                self.tables[n_end]['srid']=row_dict['srid']
                self.tables[n_end]['geom']=row_dict['geom']

        if self.debug :
            print('s.tables=',self.tables,file=sys.stderr)
        for key in ('_point','_line','_polygon','_ways','_rels') :
            assert key in self.tables and isinstance(self.tables[key],dict), 'Could not autodetect which tables contain the data'

        j_schema=list(get_columns_of_types(self.c,('jsonb',),self.tables['_rels']['name']))
        t_schema=list(get_columns_of_types(self.c,('_text',),self.tables['_rels']['name']))
        self.new_jsonb_schema=len(j_schema)==2
        if self.debug:
            print('t_schema=',t_schema,'j_schema',j_schema,file=sys.stderr)
        if self.new_jsonb_schema :
            assert len(t_schema)==0, 'Could not decide which middle db schema is used'
        else :
            assert len(t_schema)==2, 'Could not decide which middle db schema is used'

        print('INFO:','detected middle database layout:','new jsonb' if self.new_jsonb_schema else 'legacy text[]',file=sys.stderr)
        asyncio.run(self.test())
        print('[ start ]',time.strftime('%F_%T'),file=sys.stderr)
        self.has_suggested_out_filename=False #only print suggestion once


    def make_bounds_constr(self,table_key:str)->typing.Collection[str] :
        ''' Lookup the table_key in self.tables and return the
        "ST_Intersects(way, ST_MakeEnvelope(x1,y2,x2,y2))" part of a query for the specific
        table table_key, and its fully qualified name. NOTE: avoid using && operator
        because that only considers the bounding box hull of the boundary!
        Also account for SRID differences (osm data usually stored in 3857, latlon is 4326)
        with ST_Transform().
        '''
        from_rel_id=False
        tgt_srid=self.tables[table_key]['srid']
        way_column=self.tables[table_key]['geom']
        if self.bounds_geojson!=None :
            with open(self.bounds_geojson,'r') as f :
                geojson=f.read().strip()
            way_constr=f"ST_GeomFromGeoJSON('{geojson}'::jsonb)"
            #way_constr_a=f'{way_column}&&ST_Transform({way_constr},{tgt_srid})'
            way_constr=f'ST_Intersects({way_column},ST_Transform({way_constr},{tgt_srid}))'
        elif self.bounds_rel_id!=None :
            osm_rel_id=self.bounds_rel_id
            from_rel_id=True
        elif self.bounds_iso!=None :
            c_name,osm_rel_id=regions_lookup(self.bounds_iso)
            if not self.has_suggested_out_filename :
                self.has_suggested_out_filename=True
                l.log(f"Suggested output filename: '{c_name}.osm'")
            osm_rel_id=int(osm_rel_id)
            from_rel_id=True
        elif self.bounds_box!=None :
            lon_from,lat_from,lon_to,lat_to=tuple(map(float,self.bounds_box.split(',')))
            #way_constr_a=f'{way_column}&&ST_Transform(ST_MakeEnvelope({lon_from}, {lat_from}, {lon_to}, {lat_to}, 4326),{tgt_srid})'
            way_constr=f'ST_MakeEnvelope({lon_from}, {lat_from}, {lon_to}, {lat_to}, 4326)'
            way_constr=f'ST_Intersects({way_column},ST_Transform({way_constr},{tgt_srid}))'
            #way_constr=f'way && ST_Transform(ST_MakeEnvelope({lon_from}, {lat_from}, {lon_to}, {lat_to}, 4326),3857)'
        else :
            l.log('Error: no boundary provided.')
            l.log("If you are sure to export the whole planet, use --bbox='-180,-89.99,180,89.99'")
            exit(1)
        if from_rel_id :
            #relation ids are stored negative
            relbound_way_col=self.tables['_polygon']['geom']
            relbound_name=self.tables['_polygon']['name'] #stores negative osm_ids for relations
            #way_constr_a=f'{way_column}&&(SELECT relbound.{relbound_way_col} FROM {relbound_name} AS relbound WHERE osm_id={-osm_rel_id})'
            assert tgt_srid==self.tables['_polygon']['srid'], 'Unsupported cross-table different geometry SRIDs'
            way_constr=f'(SELECT relbound.way FROM planet_osm_polygon AS relbound WHERE osm_id={-osm_rel_id})'
            way_constr=f'ST_Intersects({way_column},{way_constr})'
        return way_constr,self.tables[table_key]['name']


    async def test(self) :
        ''' Test: checks if get_lonlat exsits, is executable.
            And the get_lonlat execution will crash if planet.bin.nodes is
            not readwrite or does not exist.
        '''
        result=[]
        async for i in get_latlon_str_from_flatnodes(('2185493801','3546766428'),self) :
            result.append(i)
        return result


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

    def save_clearedline(self) :
        ''' Simply write a newline at the end of the previous clearline: save it.
        Warning, will garbe output if not preceded by a clearline=True log call.
        '''
        print(end='\n',file=sys.stderr)
        #same behaviour whether followed by a clearline or not
        self.previous_clearline=False

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
        if not hasattr(g,'__anext__') :
            #async does not like 'yield from' syntax, but this works
            for i in g :
                yield i
        else :
            #is an asyncio generator
            async for i in g :
                yield i

async def get_latlon_str_from_flatnodes(osm_ids:typing.Collection[int],
        s:Settings)->typing.Iterator :
    #beware, need to exchange lonlat -> latlon
    a=await asyncio.create_subprocess_exec(s.get_lonlat_binary,s.nodes_file,
        stdout=asyncio.subprocess.PIPE,stdin=asyncio.subprocess.PIPE)
    # some osm_ids may error out. in that case get_lonlat just ignores them.
    # therefore, take the output osm_id as source of truth.

    # the last character is important: NOT number data but a separator.
    for i in osm_ids :
        a.stdin.write((str(i)+'\n').encode())
    #just to be sure
    a.stdin.write(b'\n')
    a.stdin.close()
    #read all results
    while (line:=(await a.stdout.readline()).strip().decode()) :
        #l.log('read line',line)
        x,y,osm_id=line.split(';')
        yield (osm_id,y,x)

def all_nwr_within(s:Settings,a:Accumulator) :
    # 1a) select all nodes WHERE way ST_Within(bbox);
    constr,tbl_name=s.make_bounds_constr('_point')
    l.log('executing big query on',tbl_name,'...',clearline=True)
    s.c.execute(f'SELECT osm_id FROM {tbl_name} WHERE {constr};')
    for row in g_from_cursor(s.c,verbose=True,prefix_msg=tbl_name+' ') :
        a.add('nodes',row['osm_id'])
    l.log(a.len('nodes'),'nodes within bounds')

    # 1b) select all ways,rels FROM planet_osm_polygon WHERE way ST_Within(bbox);
    constr,tbl_name=s.make_bounds_constr('_polygon')
    l.log('executing big query on',tbl_name,'...',clearline=True)
    s.c.execute(f'SELECT osm_id FROM {tbl_name} WHERE {constr};')
    for row in g_from_cursor(s.c,verbose=True,prefix_msg=tbl_name+' ') :
        id=row['osm_id']
        if id>0 :
            a.add('ways',id)
        else :
            a.add('rels',-id)
    l.log(a.len('ways'),'ways,',a.len('rels'),'rels from',tbl_name)

    # 1c) select all ways,rels FROM planet_osm_line WHERE way ST_Within(bbox);
    # planet_osm_roads is not needed in that fashion, because it is a strict subset
    # of planet_osm_line, and we're only collecting ids of objects for now
    constr,tbl_name=s.make_bounds_constr('_line')
    l.log('executing big query on',tbl_name,'...',clearline=True)
    s.c.execute(f'SELECT osm_id FROM {tbl_name} WHERE {constr};')
    for row in g_from_cursor(s.c,verbose=True,prefix_msg=tbl_name+' ') :
        id=row['osm_id']
        if id>0 :
            a.add('ways',id)
        else :
            a.add('rels',-id)
    l.log(a.len('ways'),'ways,',a.len('rels'),'rels within bounds')

    
def nodes_parent_wr(s:Settings,a:Accumulator,only_nodes_within=False) :
    # 2a) foreach node_id :
    # 2b) select all ways WHERE ARRAY[node_id]::bigint[] <@ nodes;
    # 2c) select all rels WHERE ARRAY[node_id]::bigint[] <@ parts;
    nodes_name='nodes_within' if only_nodes_within else 'nodes'
    l.log('checking parent ways of',a.len(nodes_name),'nodes')
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
        if node_count%8==0 :
            l.log(way_count,'ways,',rel_count,
                'rels parents of node',node_count,'/',a.len(nodes_name),
                '    ',percent(node_count,a.len(nodes_name)),clearline=True)
        for way in way_ids:
            way_count+=1
            a.add('ways',way['id'])
        for rel in rel_ids:
            rel_count+=1
            a.add('rels',rel['id'])
    l.log(a.len('ways'),'ways,',a.len('rels'),'rels forward from nodes')

def ways_parent_r(s:Settings,a:Accumulator) :
    # 3a) foreach way_id :
    # 3b) select all rels WHERE ARRAY[way_id]::bigint[] <@ parts;
    way_count=0
    rel_count=0
    tbl_rels=s.tables['_rels']['name']
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
        if way_count%8==0 :
            l.log(rel_count,'rels parents of way',way_count,'/',a.len('ways'),
                '    ',percent(way_count,a.len('ways')),clearline=True)
    l.log(a.len('rels'),'rels forward')

def rels_children_nwr(s:Settings,a:Accumulator,only_multipolygon_rels=False,without_rels=False) :
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
            for row in g_from_cursor(s.c) :
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
            l.log(node_count,'nodes,',way_count,'ways,',rel_count,'rels children of rel',tot_count,'/',
                a.len('rels'),'    ',percent(tot_count,a.len('rels')),clearline=True)
        if without_rels :
            l.save_clearedline()
            return #after first run
        for rel_id in buffer_add_rels :
            a.add('rels',rel_id)
        tot_count=0 #reset counter to make only count up to 100% not 200%
    l.save_clearedline()

def ways_children_n(s:Settings,a:Accumulator) :
    a_len=a.len
    # 4b) foreach way_id: add all its nodes[] ids
    way_count=0
    node_count=0
    for way_id in a.all('ways') :
        way_count+=1
        s.c.execute(f'SELECT id,nodes FROM {s.tables["_ways"]["name"]} WHERE id={way_id};')
        for row in g_from_cursor(s.c) :
            for i in row['nodes'] :
                a.add('nodes',i)
                if i==122317 :
                    l.log('n122317 is in',a.is_in('nodes',122317))
                    l.log('FROM way',row['id'],'which has',len(row['nodes']),'nodes')
                node_count+=1
        if way_count%32==0 :
            l.log(node_count,'nodes children of way',way_count,'/',a_len('ways'),
                '    ',percent(way_count,a_len('ways')),clearline=True)
    #end write line with 100%
    l.log(node_count,'nodes children of way',way_count,'/',a_len('ways'),
        '    ',percent(way_count,a_len('ways')),clearline=True)
    l.save_clearedline()


async def stream_osm_xml(s:Settings) :
    ''' Query osm2pgsql-imported postgres database for nodes, ways and rels and stream
    an xml representation of them into s.out_file. Attempts to select objects that are in
    the given bounds. But the dependencies are sometimes required, so more data that just
    within the bounds will be included. Currently, no geometric features are clipped in
    any way.
    See --help for s.bounds.
    '''
    ## TODO: move this config to Settings
    with_parents=True
    phases=['within','children','parents','write']
    if not with_parents :
        phases.remove('parents')
    l.set_phases(phases)
    #nodes within are a subset of nodes: copy of nodes just after all_nwr_within was run
    a=DictAccumulator(('nodes','nodes_within','ways','rels','done_ids'))

    #SELECT workflow to get all element [ids ONLY] in bounding box or boundary:
    all_nwr_within(s,a)
    #copy
    for i in a.all('nodes') :
        a.add('nodes_within',i)

    l.next_phase() #children

    ## TODO: move config to Settings
    #WITHOUT rel->child:rel because you end up including Novosibirsk from Switzerland
    rels_children_nwr(s,a,only_multipolygon_rels=True,without_rels=True)
    ways_children_n(s,a)

    if with_parents :
        l.next_phase() #parents
        nodes_parent_wr(s,a,only_nodes_within=True)
        #ways_parent_r(s,a)

    l.next_phase() #write
    l.log('n122317 is in',a.is_in('nodes',122317))
    counts=[a.len(i)for i in ('nodes','ways','rels')]
    l.log('dumping',counts[0],'nodes,',counts[1],'ways,',counts[2],'rels in total')

    # ONLY after all ids have been resolved, do we actually query the data,
    # RAM-inefficient otherwise; more RAM-inefficient for bigger extracts.
    # do more of a streaming from database to file approach
    out_file=sys.stdout.buffer if s.out_file=='-' else s.out_file
    with ET.xmlfile(out_file,encoding='utf-8') as xml_out :
        xml_out.write_declaration()
        with xml_out.element('osm',{
                'version':'0.6',
                'generator':time.strftime('https://github.com/feludwig/pgsql2osm/pgsql2osm.py %F')
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

def create_relations(s:Settings,a:Accumulator)->typing.Iterator[ET.Element] :
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
    l.log(a_len('done_ids'),'/',len_ids,'rels, reading table',table_name,'...')
    read_columns=[f'-{table_name}.osm_id AS id',
        f'hstore_to_json({table_name}.tags) AS json_tags',
        # we need to merge the _polygon tags with the _rels tags.
        # _planet tags are overwritten by _rels tags because
        # json_tags is first loaded, the json_tags2 overloads all existing and non-existing keys
        f'{tbl_rels}.tags AS json_tags2' if s.new_jsonb_schema else f'hstore_to_json({tbl_rels}.tags::hstore) AS json_tags2',
        # do we need this ? no ; especially the tags->'area'='yes' will overwrite 'area' if it exists
        #f'{table_name}.way_area AS area',
        #I think real does not exist and real->float4
        *list(get_columns_of_types(s.c,('int4','int','int8','int16','text','real','float4','float8'),table_name))
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
    for row_dict in g_query_ids(s.c,query,g_negate(a.all('rels')),'osm_id',step=250) :
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
        l.log(a_len('done_ids'),'/',len_ids,'rels','    ',percent(a_len('done_ids'),len_ids),clearline=True)

    #and now with _line as well
    table_name=s.tables['_line']['name']
    l.log(a_len('done_ids'),'/',len_ids,'rels, reading table',table_name,'...')

    read_columns=[f'-{table_name}.osm_id AS id',
        f'hstore_to_json({table_name}.tags) AS json_tags',
        #f'{tbl_rels}.tags AS json_tags2' if s.new_jsonb_schema else f'hstore_to_json({tbl_rels}.tags::hstore) AS json_tags2',
        *list(get_columns_of_types(s.c,('int4','int','int8','int16','text','real','float4','float8'),table_name))
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
    for row_dict in g_query_ids(s.c,query,g_negate(a.all_subtract('rels','done_ids')),'osm_id',step=250) :
        if first :
            start_t=time.time()
            l.log('rels _line output start',start_t)
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
        l.log(a_len('done_ids'),'/',len_ids,'rels','    ',percent(a_len('done_ids'),len_ids),clearline=True)
    l.log('rels _line output end',(time.time()-start_t))

    table_name=tbl_rels
    l.log(a_len('done_ids'),'/',len_ids,'rels, reading table',table_name,'...')
    if s.new_jsonb_schema :
        query=f'SELECT id,members,tags AS json_tags FROM {table_name}'
    else :
        #in this table, tags is ::text[], not a hstore
        query=f'SELECT id,members,hstore_to_json(tags::hstore) AS json_tags FROM {table_name}'
    #bigger step than previous, because there is (heurisitcally) less data for these "light" relations,
    # which have no interesting tags regarding rendering making them worthy of a place in _polygon or _line
    if s.debug_xml :
        yield ET.Element('debug',{'status':'starting rels query'})
    for row_dict in g_query_ids(s.c,query,a.all_subtract('rels','done_ids'),'id',step=300) :
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
        l.log(a_len('done_ids'),'/',len_ids,'rels','    ',
                percent(a_len('done_ids'),len_ids),clearline=True)
    l.log(a_len('done_ids'),'/',len_ids,'rels','    ',
            percent(a_len('done_ids'),len_ids),clearline=True)
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

def create_ways(s:Settings,a:Accumulator)->typing.Iterator[ET.Element] :
    tbl_ways=s.tables['_ways']['name']
    table_name=s.tables['_polygon']['name']
    a_add=a.add
    a_len=a.len
    add_done_ids=lambda i:a_add('done_ids',i)
    a.clear('done_ids')
    len_ids=a_len('ways')

    l.log(a_len('done_ids'),'/',len_ids,'ways, reading table',table_name,'...')
    read_columns=[f'{table_name}.osm_id AS id',
        f'hstore_to_json({table_name}.tags) AS json_tags', #polygons stores a hstore even in the new_jsonb_schema
        f'{tbl_ways}.tags AS json_tags2' if s.new_jsonb_schema else f'hstore_to_json({tbl_ways}.tags::hstore) AS json_tags2',
        # do we need this ? no
        #f'{table_name}.way_area AS area',
        #I think real does not exist and real->float4
        *list(get_columns_of_types(s.c,('int4','int','int8','int16','text','real','float4','float8'),table_name))
    ]
    query='SELECT '+(','.join(read_columns))
    query+=f',{tbl_ways}.nodes FROM {table_name} JOIN {tbl_ways}'
    query+=f' ON {table_name}.osm_id={tbl_ways}.id'

    for row_dict in g_query_ids(s.c,query,a.all('ways'),'osm_id') :
        if a.is_in('done_ids',row_dict['id']) :
            continue
        #collapse hstore tags 
        tags=row_dict.pop('json_tags') if 'json_tags' in row_dict else {}
        tags={**tags,**row_dict.pop('json_tags2')} if 'json_tags2' in row_dict else tags
        yield way_to_xml(row_dict,tags)
        add_done_ids(row_dict['id'])
        l.log(a_len('done_ids'),'/',len_ids,'ways','    ',percent(a_len('done_ids'),len_ids),clearline=True)

    #and now with _line
    table_name=s.tables['_line']['name']
    l.log(a_len('done_ids'),'/',len_ids,'ways, reading table',table_name,'...')
    read_columns=[f'{table_name}.osm_id AS id',
        f'hstore_to_json({table_name}.tags) AS json_tags',
        f'{tbl_ways}.tags AS json_tags2' if s.new_jsonb_schema else f'hstore_to_json({tbl_ways}.tags::hstore) AS json_tags2',
        *list(get_columns_of_types(s.c,('int4','int','int8','int16','text','real','float4','float8'),table_name))
    ]
    query='SELECT '+(','.join(read_columns))
    query+=f',{tbl_ways}.nodes FROM {table_name} JOIN {tbl_ways}'
    query+=f' ON {table_name}.osm_id={tbl_ways}.id'

    for row_dict in g_query_ids(s.c,query,a.all_subtract('ways','done_ids'),'osm_id') :
        if a.is_in('done_ids',row_dict['id']) :
            continue
        #collapse hstore tags 
        tags=row_dict.pop('json_tags') if 'json_tags' in row_dict else {}
        tags={**tags,**row_dict.pop('json_tags2')} if 'json_tags2' in row_dict else tags
        yield way_to_xml(row_dict,tags)
        add_done_ids(row_dict['id'])
        l.log(a_len('done_ids'),'/',len_ids,'ways','    ',percent(a_len('done_ids'),len_ids),clearline=True)


    #everything else was queried, only _ways remains
    table_name=tbl_ways
    l.log(a_len('done_ids'),'/',len_ids,'ways, reading table',table_name,'...')
    #in this table, tags::text[], not yet a hstore
    if s.new_jsonb_schema :
        query=f'SELECT id,nodes,tags AS json_tags FROM {table_name}'
    else :
        query=f'SELECT id,nodes,hstore_to_json(tags::hstore) AS json_tags FROM {table_name}'
    for row_dict in g_query_ids(s.c,query,a.all_subtract('ways','done_ids'),'id') :
        if a.is_in('done_ids',row_dict['id']) :
            continue
        #collapse hstore tags 
        tags=row_dict.pop('json_tags') if 'json_tags' in row_dict else {}
        yield way_to_xml(row_dict,tags)
        add_done_ids(row_dict['id'])
        l.log(a_len('done_ids'),'/',len_ids,'ways','    ',percent(a_len('done_ids'),len_ids),clearline=True)
    a.clear('ways')

def g_negate(g:typing.Iterator[int]) :
    for i in g :
        yield -i

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
    if verbose:
        count-=1
        l.log(prefix_msg+'row',count,'/',tot_count,'    ',percent(count,tot_count),clearline=True)

def g_query_ids(c:psycopg2.extensions.cursor,query:str,ids:typing.Iterator[int],
        id_col:str,step=1000,verbose=False)->typing.Iterator[dict] :
    ''' Given an SQL query without the ending semicolon and where the last
    clause is a WHERE, append AND {id_col} IN (*ids) and yield those results.
    The parenthesizing should be made explicit, NO GUARANTEER in this case:
    [...] WHERE condA OR condB -> [...] WHERE condA OR condB AND id IN ([...])
    -> SHOULD write [...] WHERE (condA OR condB)
    A psql syntax error will be thrown if ORDER BY, LIMIT are the last clause.
    '''
    init_query=query
    # case of 'SELECT ... FROM table' -> 'SELECT .. FROM table WHERE {append_query}'
    with_and='AND'
    if init_query.find('WHERE')<0 :
        init_query+=' WHERE'
        with_and='' #remove the AND

    finished=False
    while not finished :
        #fetch step many values
        buf=[]
        for i in range(step) :
            try :
                buf.append(str(next(ids)))
            except StopIteration :
                finished=True # out of the while
                break #out of the for 
        if len(buf)==0 :
            continue
        query=f'{init_query} {with_and} {id_col} IN ('
        query+=','.join(buf)
        query+=');'
        if verbose :
            l.log(query)
        c.execute(query)
        if verbose :
            l.log('query returned',c.rowcount,'rows')
        yield from g_from_cursor(c)

def get_columns_of_types(c:psycopg2.extensions.cursor,
        col_types:typing.Collection[str],table_full_name:str)->typing.Iterator[str] :
    values_col_types=",".join(["'"+i+"'" for i in col_types])
    schema_name,table_name=table_full_name.strip('"').split('.')
    c.execute(f'''
    SELECT colname,strtype FROM 
        (SELECT attname AS colname,
            (SELECT typname FROM pg_type WHERE oid=atttypid) AS strtype
        FROM pg_attribute WHERE attrelid=
            (SELECT oid FROM pg_class WHERE relname='{table_name}'
            AND relnamespace=(SELECT oid FROM pg_namespace
                WHERE nspname='{schema_name}'
            ))
        ) AS columns
    WHERE strtype IN ({values_col_types})
        AND colname NOT IN ('osm_id');''')
    for colname,strtype in c.fetchall() :
        yield table_name+'.'+('"'+colname+'"' if colname.find(':')>0 else colname)

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

async def create_nodes(s:Settings,a:Accumulator)->typing.Iterator[ET.Element] :
    table_name=s.tables['_point']['name']
    a.clear('done_ids')
    a_add=a.add
    a_len=a.len
    add_done_ids=lambda i:a_add('done_ids',i)
    len_ids=a_len('nodes')

    l.log(a_len('done_ids'),'/',len_ids,'nodes, reading table',table_name,'...',clearline=True)
    read_columns=[f'{table_name}.osm_id AS id',
        f'hstore_to_json({table_name}.tags) AS json_tags',
        f'ST_X(ST_Transform({table_name}.way,4326)) AS lon',
        f'ST_Y(ST_Transform({table_name}.way,4326)) AS lat',
        *list(get_columns_of_types(s.c,('int4','int','int8','int16','text'),table_name))
    ]
    query='SELECT '+(','.join(read_columns))+f' FROM {table_name}'

    for row_dict in g_query_ids(s.c,query,a.all('nodes'),'osm_id') :
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
        if a_len('done_ids')%32==0 :
            l.log(a_len('done_ids'),'/',len_ids,'nodes','    ',
                percent(a_len('done_ids'),len_ids),clearline=True)
    l.log('now querying flatnodes file for missing nodes')
    to_get_lat_lons=set()

    for batch in g_batches(a.all_subtract('nodes','done_ids'),5_000) :
        async for osm_id,lat,lon in get_latlon_str_from_flatnodes(batch,s) :
            #osm_id,lat and lon are already strings (don't bother to convert+reconvert them)
            osm_id_int=int(osm_id)
            if a.is_in('done_ids',osm_id_int) :
                continue
            yield node_to_xml({'id':osm_id,'lat':lat,'lon':lon},{})
            add_done_ids(osm_id_int)
            if a_len('done_ids')%16==0 :
                l.log(a_len('done_ids'),'/',len_ids,'nodes','    ',
                    percent(a_len('done_ids'),len_ids),clearline=True)
    a.clear('nodes')
    l.log(a_len('done_ids'),'/',len_ids,'nodes','    ',
        percent(a_len('done_ids'),len_ids),clearline=True)

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
    parser.add_argument('-d','--dsn',dest='postgres_dsn',
        default='dbname=gis',
        help="The connection string to pass to psycopg2 eg 'host=localhost dbname=gis port=5432'")

    #one of the following:
    parser.add_argument('-b','--bbox',dest='bounds_box',
        default=None,type=str,
        help='Rectangle boundary in the format lon_from,lat_from,lon_to,lat_to')
    parser.add_argument('-r','--osm-rel-id',dest='bounds_rel_id',
        default=None,type=int,
        help='Integer for the osm relation that should make the boundary')
    parser.add_argument('-i','--iso',dest='bounds_iso',
        default=None,
        help='Country or region code for looking up in regions.csv, to determine boundary')
    parser.add_argument('-g','--geojson',dest='bounds_geojson',
        default=None,
        help='Geojson file for determining the boundary')

    parser.add_argument('-o','--output',dest='out_file',
        help="Path where the output .osm should be written to. When '-', write to stdout",
        required=True)

    parser.add_argument('--debug',dest='debug',default=False,
        action='store_true',
        help='Show additional debugging information')


    args=parser.parse_args()
    s=Settings(args)
    t=asyncio.run(stream_osm_xml(s))
