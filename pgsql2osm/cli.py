#!/usr/bin/python3

import argparse
from . import pgsql2osm
from . import settings

def main() :
    parser=argparse.ArgumentParser(prog='pgsql2osm')

    parser.add_argument('get_lonlat_binary',
        help="Path to the get_lonlat binary")
    parser.add_argument('nodes_file',
        help='Path to the nodes file created by osm2pgsql at import')
    parser.add_argument('-d','--dsn',dest='postgres_dsn',
        default='dbname=gis port=5432',
        help="The connection string to pass to psycopg2, default '%(default)s'")

    parser.add_argument('-b','--bbox',dest='bounds_box',
        default=None,type=str,
        help='''Rectangle boundary in the format lon_from,lat_from,lon_to,lat_to.
Can be specified in addition to other boundaries, and will then extract the intersection.
Info: use quotes with negative numbers, eg --bbox='-180,-89,180,89'.''')
    #one of the following:
    bounds_g=parser.add_mutually_exclusive_group(required=True)
    bounds_g.add_argument('-r','--osm-rel-id',dest='bounds_rel_id',
        default=None,type=int,
        help='Integer for the osm relation that should make the boundary')
    bounds_g.add_argument('-i','--iso',dest='bounds_iso',
        default=None,
        help='Country or region code for looking up in regions.csv, to determine boundary')
    bounds_g.add_argument('-g','--geojson',dest='bounds_geojson',
        default=None,
        help='Geojson file for determining the boundary')

    parser.add_argument('-o','--output',dest='out_file',
        help="Path where the output .osm should be written to. When '-', write to stdout",
        required=True)

    parser.add_argument('--debug',dest='debug',default=False,
        action='store_true',
        help='Show additional debugging information')


    args=parser.parse_args()
    s=settings.Settings(args)
    s.main()
