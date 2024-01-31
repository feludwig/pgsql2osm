#!/usr/bin/python3

import typing
import argparse
import psycopg2
import asyncio
import os
import sys #maybe move all to log.py

from . import pgsql2osm
from . import dbutils
from . import log
from . import __metadata__


class Settings :
    def __init__(self,args:argparse.Namespace) :
        self.project_url=__metadata__['Project-URL']

        self.debug=args.debug
        self.debug_xml=False

        self.bounds_geojson=args.bounds_geojson
        self.bounds_rel_id=args.bounds_rel_id
        self.bounds_iso=args.bounds_iso
        self.bounds_box=args.bounds_box

        self.get_lonlat_binary=args.get_lonlat_binary
        self.nodes_file=args.nodes_file

        #can either be a file-obj or a filename:str
        self.out_file=sys.stdout.buffer if args.out_file=='-' else args.out_file
        
        self.access=psycopg2.connect(args.postgres_dsn)

        self.has_suggested_out_filename=False #only print suggestion once
        self.connect_and_check()

    def connect_and_check(self) :
        #use one cursor for everything
        self.c=self.access.cursor()
        self.c.execute('''SELECT f_table_name AS name,f_table_schema AS schema,
                f_geometry_column AS geom,srid,
                pg_table_size(f_table_name::text) AS size
            FROM geometry_columns WHERE srid!=0''')
        autodetect_tables=list(dbutils.g_from_cursor(self.c))
        self.c.execute('''SELECT tablename AS name,schemaname AS schema,
                pg_table_size(tablename::text) AS size
            FROM pg_tables WHERE schemaname NOT IN ('information_schema','pg_catalog')''')
        autodetect_tables.extend(list(dbutils.g_from_cursor(self.c)))
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

        j_schema=list(dbutils.get_columns_of_types(self.c,('jsonb',),self.tables['_rels']['name']))
        t_schema=list(dbutils.get_columns_of_types(self.c,('_text',),self.tables['_rels']['name']))
        self.new_jsonb_schema=len(j_schema)==2
        if self.debug:
            print('t_schema=',t_schema,'j_schema',j_schema,file=sys.stderr)
        if self.new_jsonb_schema :
            assert len(t_schema)==0, 'Could not decide which middle db schema is used'
        else :
            assert len(t_schema)==2, 'Could not decide which middle db schema is used'

        log.l.log_start('INFO: detected middle database layout = '+('new jsonb' if self.new_jsonb_schema else 'legacy text[]'))
        asyncio.run(self.test())


    def make_bounds_constr(self,table_key:str)->typing.Collection[str] :
        """ Lookup the table_key in self.tables and return the
        "ST_Intersects(way, ST_MakeEnvelope(x1,y2,x2,y2))" part of a query for the specific
        table table_key, and its fully qualified name. NOTE: avoid using && operator
        because that only considers the bounding box hull of the boundary!
        Also account for SRID differences (osm data usually stored in 3857, latlon is 4326)
        with ST_Transform().
        ALSO: bbox can be specified in addition to any of the other bounds: make an
        intersection then
        """
        from_rel_id=False
        tgt_srid=self.tables[table_key]['srid']
        way_column=self.tables[table_key]['geom']
        way_constr=None
        if self.bounds_geojson!=None :
            with open(self.bounds_geojson,'r') as f :
                geojson=f.read().strip()
            way_constr=f"ST_GeomFromGeoJSON('{geojson}'::jsonb)"
            way_constr=f'ST_Intersects({way_column},ST_Transform({way_constr},{tgt_srid}))'
        elif self.bounds_rel_id!=None :
            osm_rel_id=self.bounds_rel_id
            from_rel_id=True
        elif self.bounds_iso!=None :
            c_name,osm_rel_id=dbutils.regions_lookup(self.bounds_iso)
            if not self.has_suggested_out_filename :
                self.has_suggested_out_filename=True
                l.log(f"Suggested output filename: '{c_name}.osm'")
            osm_rel_id=int(osm_rel_id)
            from_rel_id=True

        if from_rel_id :
            relbound_way_col=self.tables['_polygon']['geom']
            relbound_name=self.tables['_polygon']['name'] #stores negative osm_ids for relations
            assert tgt_srid==self.tables['_polygon']['srid'], 'Unsupported cross-table different geometry SRIDs'
            #relation ids are stored negative
            way_constr=f'(SELECT relbound.{relbound_way_col} FROM {relbound_name} AS relbound WHERE osm_id={-osm_rel_id})'
            way_constr=f'ST_Intersects({way_column},{way_constr})'
        
        if self.bounds_box!=None and way_constr==None :
            lon_from,lat_from,lon_to,lat_to=tuple(map(float,self.bounds_box.split(',')))
            way_constr=f'ST_MakeEnvelope({lon_from}, {lat_from}, {lon_to}, {lat_to}, 4326)'
            way_constr=f'ST_Intersects({way_column},ST_Transform({way_constr},{tgt_srid}))'
        elif self.bounds_box!=None and way_constr!=None :
            # INTERSECTION between already bounds and current bbox:
            # actually doing an ST_IntersectION means postgresql cannot optimize it as well.
            # the AND structure is much faster (and query planner agrees as well) ->
            #   cost 1000x lower from 188'000 to 239
            lon_from,lat_from,lon_to,lat_to=tuple(map(float,self.bounds_box.split(',')))
            way_constr_bbox=f'ST_MakeEnvelope({lon_from}, {lat_from}, {lon_to}, {lat_to}, 4326)'
            way_constr_bbox=f'ST_Intersects({way_column},ST_Transform({way_constr_bbox},{tgt_srid}))'
            way_constr=f'{way_constr} AND {way_constr_bbox}'

        if way_constr==None :
            l.log('Error: no boundary provided.')
            l.log("If you are sure to export the whole planet, use --bbox='-180,-89.99,180,89.99'")
            exit(1)
        return way_constr,self.tables[table_key]['name']

    def main(self) :
        """ Handle all the asyncio stuff for stream_osm_xml(), only returns when
        everything is finished. Can be run multiple times
        """
        try :
            t=asyncio.run(pgsql2osm.stream_osm_xml(self))
        except ZeroDivisionError :
            print('\nError: boundary is empty or database has no data within',file=sys.stderr)
        sys.stderr.flush()

    async def test(self) :
        """ Test: checks if get_lonlat exsits, is executable.
            And the get_lonlat execution will crash if planet.bin.nodes is
            not readwrite or does not exist.
        """
        if not os.path.exists(self.get_lonlat_binary) :
            raise BaseException(f'Did not find get_lonlat_binary at {self.get_lonlat_binary}')
        #check that user=execute bit is set
        elif not ((os.stat(self.get_lonlat_binary).st_mode>>6)%8)%2==1 :
            raise BaseException(f'Seems not executable: get_lonlat_binary, please run "chmod +x {self.get_lonlat_binary}"')
        if not os.path.exists(self.nodes_file) :
            raise BaseException(f'Did not find nodes_file at {self.nodes_file}')

        result=[]
        async for i in dbutils.get_latlon_str_from_flatnodes(('2185493801','3546766428'),self) :
            result.append(i)
        #WARNING: result may be empty and then what?
        return result

class ModuleSettings(Settings) :
    """ Pass the stream_osm_xml() a ModuleSettings if you directly want to
    give python objects instead of using the CLI and argparse
        * supported feature: 'out_file' can be any open file python object
    WARNING: danger zone, requirement is not checked! if you forget to set some
    settings value, this script may crash just before the end!
    """
    def __init__(self,**kwargs):
        self.project_url=__metadata__['Project-URL']
        #something not in keys will be ignored, dict value is DEFAULT value
        keys={'debug':False,'debug_xml':False,'bounds_geojson':None,
                'bounds_rel_id':None,'bounds_iso':None,'bounds_box':None,
                'get_lonlat_binary':None,'nodes_file':None,'out_file':None,
                'access':None,'postgres_dsn':None,'has_suggested_out_filename':False,
        }
        for k,v in kwargs.items() :
            if k in keys :
                setattr(self,k,v)
        for k,v in keys.items() :
            if not hasattr(self,k) :
                setattr(self,k,v)
        self.connect_and_check()
