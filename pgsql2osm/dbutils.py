#!/usr/bin/python3

import psycopg2
import typing
import asyncio
import os

from . import log


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
    log.l.log_start(f'Error iso boundary not found: {isocode}')
    exit(1)

async def get_latlon_str_from_flatnodes(osm_ids:typing.Collection[int],s)->typing.Iterator :
    """ s is a settings.Settings object
    """
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
        #beware, need to exchange lonlat -> latlon
        yield (osm_id,y,x)

def g_from_cursor(c:psycopg2.extensions.cursor,verbose=False,prefix_msg='')->typing.Iterator[dict]:
    ''' Assuming the query has already c.execute()d, return its results
    as a dict-generator'''
    columns=[i.name for i in c.description]
    tot_count=c.rowcount
    count=1 # for human display
    while (row:=c.fetchone())!=None :
        if verbose :
            log.l.simplerate(count,prefix_msg+'row',tot_count)
        yield {k:v for k,v in zip(columns,row) if v!=None}
        count+=1
    if verbose:
        log.l.finishrate()
        #count-=1
        #l.log(prefix_msg+'row',n(count),'/',n(tot_count),'    ',percent(count,tot_count),clearline=True)

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
            log.l.log(query)
        c.execute(query)
        if verbose :
            log.l.log('query returned',c.rowcount,'rows')
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

