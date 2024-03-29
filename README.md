# pgsql2osm
Export .osm from a PostgreSQL openstreetmap database

## Motivation

You have access to a PostgreSQL+PostGIS database which contains openstreetmap
data imported by
[`osm2pgsql`](https://osm2pgsql.org/)
for rendering maps. How can you generate an extract
to use with other cartography tools ?


`pgsql2osm` is a python script that can generate the XML in `.osm` format
for all entities within given boundaries.
It attempts to preserve all data attributes from original osm data:
nodes, ways and relations and all their `osm_id`s, all tags, and
interreferences (references to the outside of the extract you choose will be broken,
unless you use `--bbox='-180,-89.99,180,89.99'` to select all available data).


The main usecase I have for this is to generate an `.obf` file using
[osm2obf](https://github.com/feludwig/osm2obf)
for the Android
[OsmAnd](https://osmand.net/)
app offline maps.

## Requirements

#### Database

* was imported with `osm2pgsql`
    - in `--slim` mode (this allows incremental updates and is also leveraged by `pgsql2osm.py`)
    - _Recommended_ : with `--hstore` containing all remaining tags
not saved as columns in the `tags::hstore`. If not, the produced `.osm` will
have incomplete tags (but will be geometrically sound)
    - with `--flat-nodes`, and access to the `--flat-nodes <FILE>` cache binary file
(for now, must be readwrite)

* The geometry tables are the first with names that end
in `_point`, `_line`, `_polygon` listed in the following query:
```
SELECT f_table_name,f_geometry_column,srid,pg_size_pretty(pg_table_size(f_table_name::text))
FROM geometry_columns WHERE srid!=0 ORDER BY pg_table_size(f_table_name::text) DESC LIMIT 10;
```
* Same for `_ways` and `_rels` tables:
```
SELECT relname,relkind,pg_size_pretty(pg_table_size(relname::text)),
    (SELECT nspname FROM pg_namespace WHERE oid=relnamespace) AS namespace
FROM pg_class WHERE relkind IN ('r','m','v') AND relnamespace NOT IN (SELECT oid FROM pg_namespace
        WHERE nspname IN ('pg_toast','pg_catalog','information_schema'))
    AND (relname~'_rels' OR relname~'_ways')
ORDER BY pg_table_size(relname::text) DESC;
```


## Features

* XML streaming output, allows to only store compressed .osm.bz2 on disk and not waste RAM.
Example:
```
pgsql2osm --dsn 'dbname=gis' --iso fr --output -|bzip2 > France.osm.bz2
```
* Attempt to lower RAM footprint with generators for database queries:
streaming all the way from database to XML
* Automatic detection of table names, referred to here as `planet_osm_*` , but they can also
be named anything with the correct suffixes added by `osm2pgsql`: `_point`, `_line`, `_polygon`,
`_ways` and `_rels`
* Automatic detection of the middle database format (legacy text[] or new jsonb,
available in databases created by `osm2pgsql`>=1.9). WARNING: very old versions <1.3 may use a
different middle database format, it was undocumented at that time. In that case it is recommended to
re-import with a new `osm2pgsql` version.
* Automatic detection of `planet_osm_point`, `planet_osm_line` and
`planet_osm_polygon` columns: a specific `.style` at import is not required
  - _Warning_ : The column names you choose will be the keys in the `.osm` output
regardless of the original tag's key. When not using default tag transforms, a `.osm` file will be produced that
may not have the tags like they are expected by other software.
  - _Note_ : key-values in `tags` override column key-values

* Bounds specifiable as _either_ :
  - iso country or region code `--iso de-by` for Germany/Bayern
    * `grep -i MY regions.csv` can show you recognized iso codes for Malaysia and subregions
  - osm relation id `--osm-rel-id N`, for any relation in the database.
  - geojson polygon input file `--geojson file.geojson`
  - bounding box `--bbox='<lon_from>,<lat_from>,<lon_to>,<lat_to>'`
* Bounds intersection can also be specified as one non-`bbox` of the above and a `--bbox`.
The extracted region will then be the intersection (logical AND) of the shape with the bbox.
* Anti-Feature: unsorted ids, see [Unsorted ids](#unsorted-ids)

### Benchmarks 

Extract | File size | `time` | elements count | RAM
---|---|---|---|---
Austria-left15\_0.osm.bz2<br>`--bbox='-180,-89,15.01,89'`|681MB|real 2h18<br>user 37min<br>sys 42min|46.7M n<br>4.54M w<br>75K r|10.6GB
Austria-right15\_0.osm.bz2<br>`--bbox='14.99,-89,180,89'`|502MB|real 1h23<br>user 23min<br>sys 17min|33.1M n<br>4.01M w<br>67K r| -
Baden-württemberg.osm.bz2<br>`--bbox='-180,-89,180,48.31'`|274MB|real 49min<br>user 16min<br>sys 5min<br>|17.2M n<br>2.53M w<br>32.6K r| -
Baden-württemberg.osm.bz2<br>`--bbox='-180,48.29,180,89'`|560MB|real 2h08<br>user 34min<br>sys 21min<br>|34.0M n<br>5.84M w<br>63K r|6.1GB
Austria-left14.25.osm.bz2<br>`--bbox='-180,-89,14.26,89'`|509MB|real 1h21<br>user 21min<br>sys 22min|34.6M n<br>3.31M w<br>53K r|5.7GB
Austria-right14.25.osm.bz2<br>`--bbox='14.24,-89,180,89'`|676MB|real 2h11<br>user 30min<br>sys 46min|45.4M n<br>5.27M w<br>89K r|10.7GB
Switzerland.osm.bz2|692MB|real 2h35<br>user 39min<br>sys 45min|47.1M n<br>5.3M w<br>94.5K r| -
Germany-bavaria.osm.bz2|1.1GB|real 5h17<br>user 1h06<br>sys 1h35|70.1M n<br>10.5M w<br>95.8K r|12.1GB


_Note_ : The File size is for the compressed extract, bzip2 default settings used unless
otherwise noted

#### RAM usage monitoring

I just run the following to get the max RAM used by that process 

```
maxmemkb=0;while sleep 1;do
  memkb="$(ps -Ao rss,cmd|grep python3|grep pgsql2osm|cut -d' ' -f1)";
  if [ "${maxmemkb}" -lt "${memkb}" ];then maxmemkb="${memkb}";fi;
  printf '\033[2K\r%s max=%s' "${memkb}" "${maxmemkb}";done
```

## Disclaimers

### Outside of bounds geometries
The bounding region will include every entity and all its dependent entities.
This also means some entities outside the boundary will be inlcuded, as long as
they have any reference to entities within the boundary.
No geometry clipping is taking place, entities are exported as-is and can extend
hundreds of kilometers beyond the boundary if that is a continuous entity
(eg a long river).

### Unsorted ids

The output ids are streamed in the order they come from the database, and for speed reasons
this is unsorted. If you need a sorted `.osm`, use
[`osmium`](https://osmcode.org/osmium-tool/) after exporting:


```
osmium sort in.osm.bz2 --output=out.osm.bz2
```


### Database does not store all osm data strictly

The output data may have some missing tags, because the database import
is not a lossless operation: importing a `.osm.pbf` to database, then exporting
it to `.osm` and then converting it back to `.osm.pbf` will most probably
(on real world data) produce a **different** file.


# Installation

### `pgsql2osm`
```
pip install pgsql2osm
```

### `get_lonlat` utility

* Need to compile
`osm2pgsql` ([instructions](https://github.com/osm2pgsql-dev/osm2pgsql#building))
* then copy `get_lonlat.cpp` to `osm2pgsql/src/get_lonlat.cpp`
* and in the `osm2pgsql/build/` directory:
```
make
```
```
g++ -std=c++17 ../src/get_lonlat.cpp ../src/node-persistent-cache.cpp \
-I ../contrib/fmt/include/ ../build/src/libosm2pgsql.a -Wall -g -o get_lonlat
```
* Test: 
```
./get_lonlat /path/to/planet.bin.nodes <<< 3546766428
```
* Should output `39.188565;49.885284;3546766428` if that node was covered by database import

### Run

```
pgsql2osm /path/to/get_lonlat /path/to/planet.bin.nodes --help
```
For an overview of options

# Python module

Made easier with the ModuleSettings class, example:

```
import pgsql2osm
m=pgsql2osm.settings.ModuleSettings(
  bounds_rel_id=osm_rel_id,
  get_lonlat_binary='/path/to/get_lonlat',
  nodes_file='/path/to/planet.bin.nodes',
  # no need for the --dsn if access already from somewhere else
  access=already_previously_obtained_dbaccess)
with bz2.open('test.osm.bz2','wb') as f:
  m.out_file=f #set the output file object
  m.main()
```
__Note__: will still print progress reports to stderr

## Implementation details

### Database details

* `osm2pgsql`'s `--slim` mode means: has the "middle" layout tables containing all the raw osm references between
`ways->nodes` and `rels->nodes,ways,rels`, more specifically:
* The "middle" database tables have names ending in `_ways` and `_rels`
(`osm2pgsql` defaults),
and are the biggest with that suffix in the database, by used space
* The `*_ways` table has columns
  - `id::bigint` for the osm ID,
  - `nodes::bigint[]` for a list of the way's child nodes,
  - and `tags::text[]` for that way's tags in hstore format,
  - with `tags::jsonb` in the case of the new jsonb format.
* The `*_rels` table has columns
  - `id::bigint` for the osm ID,
  - in the case of legacy text[] format:
    * `parts::bigint[]` any ids of (mixed) nodes,
ways or relations that are children of that relation,
    * `tags::text[]` for the relation's tags in hstore format,
    * `members::text[]` that more precisely describes the children,
example `{'n123','admin_centre','w345','outer','w567','inner'}`
having the node `123` as a role `admin_centre` and the ways `345` and `567`,
as roles `outer` and `inner` boundaries respectively
  - in the case of new jsonb format:
    * `members::jsonb` a list of json objects describing the members, the example continued
`[{"ref":"123","role":"admin_centre","type":"N"},{"ref":"345","role":"outer","type":"W"},
{"ref":"567","role":"inner","type":"W"}]`,
    * and `tags::jsonb` for the relation's tags in `{"key1":"value1","key2":"value2"}` format,

* Access needed: `SELECT` granted on the above mentioned tables and also
(for the autodetections of table names and column names)
`pg_attribute`, `pg_tables`, `pg_type`, `pg_class`, and `geometry_columns`.

#### Regions.csv

To generate `regions.csv`, run in a planet database:

```
psql -d gis -p 5432 --csv -c "select -osm_id as osm_id,coalesce(tags->'name:en',name) as name,
    tags->'ISO3166-1' as iso_country,tags->'border_type' as border_type,
    tags->'ISO3166-2' as iso_subcountry,admin_level,tags->'wikipedia' as wikipedia
  from planet_osm_polygon where osm_id<0 and admin_level in ('1','2','3','4')
    and boundary='administrative'" > regions.csv
```


_Note_ : only `admin_level`<=4 is loaded into `regions.csv`.
Use a search on [openstreetmap.org](https://www.openstreetmap.org) for smaller regions.
And then extract the relation id from the url:
<https://www.openstreetmap.org/relation/51701> `51701`,
eg Switzerland

