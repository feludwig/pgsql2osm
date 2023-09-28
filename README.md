# pgsql2osm
Export .osm from a PostgreSQL openstreetmap database

Motivation
---

You have access to a PostgreSQL+PostGIS database which contains openstreetmap
data imported by
[`osm2pgsql`](https://osm2pgsql.org/)
for rendering maps. How can you generate an extract
to use with other cartography tools ?
`pgsql2osm.py` is an experimental tool that can generate the XML in `.osm` format
for all entities within given boundaries.


The main usecase I have for this is to generate an `.obf` file for the Android
[OsmAnd](https://osmand.net/)
app, chained with
[`OsmAndMapCreator`](https://wiki.openstreetmap.org/wiki/OsmAndMapCreator)
([guide](https://github.com/osmandapp/web/blob/main/main/docs/technical/map-creation/create-offline-maps-yourself.md))
for `.osm` -> `.obf`.

Requirements
---

* `python3`
  - `pip install psycopg2`
* database was imported with `osm2pgsql` and
  - is imported in `--slim` mode (this allows incremental updates and is also leveraged by `pgsql2osm.py`)
<=> has the "middle" layout tables containing all the osm references between
`ways->nodes` and `rels->nodes,ways,rels`,
  - _Recommended_ : was imported with `--hstore` containing all remaining tags
not saved as columns in the `tags` hstore
  - The geometry tables' names end with `\_point`, `\_line`, `\_polygon`
and have a nonzero `srid` for their respective geometry column
(these are `osm2pgsql` default settings).
To check, see that those tables are present in the result of the following:


`SELECT f_table_name,f_geometry_column,srid FROM geometry_columns WHERE srid!=0;`

  - The "middle" database tables have names ending in `_ways` and `_rels`
(`osm2pgsql` defaults), and are
the biggest in the database by used space
  - with `--flat-nodes`, and access to the `--flat-nodes <FILE>` cache binary file (for now, must be readwrite)
* `SELECT` access granted on the above mentioned tables and also
(for the autodetections of table names and column names)
`pg_attribute`, `pg_tables`, `pg_type`, `pg_class`, and `geometry_columns`.

Features
---

* XML streaming output, allows to only store compressed .osm.bz2 on disk and not waste RAM.
Example:


`python3 -u pgsql2osm.py --dsn 'user=username dbname=gis' --iso fr --output -|bzip2 > France.osm.bz2`
* Attempt to lower RAM footprint with generators for database queries:
streaming all the way from database to XML
* Automatic detection of table names, referred to here as `planet_osm_* , but they can also
be named anything with the correct suffixes added by `osm2pgsql`: `\_point`, `\_line`, `\_polygon`,
`\_ways` and `\_rels`
* Automatic detection of the middle database format (legacy text[] or new jsonb,
available in databases created by `osm2pgsql`>=1.9)
* Automatic detection of `planet_osm_point`, `planet_osm_line` and
`planet_osm_polygon` columns: a specific `.style` at import is not required
* Bounds specifiable as _either_ :
  - iso country or region code `--iso de-by` for Germany/Bayern
    * `grep -i MY regions.csv` can show you recognized iso codes for Malaysia and subregions
  - osm relation id `--osm-rel-id N`, for any relation in the database.
  - geojson polygon input file `--geojson file.geojson`
  - bounding box `--box='<lon_from>,<lat_from>,<lon_to>,<lat_to>'`
* Anti-Feature: unsorted ids, see [Unsorted ids](#unsorted-ids)

### Benchmarks 

* `Switzerland.osm.bz2` is `694MB` in `real 1h08min/user 32mim/sys 22min`
* `Germany-Bavaria.osm.bz2` is `2.2GB`, in `real 6h49min/user 1h36min/sys 2h49min`.

Disclaimer
---

### Outside of bounds geometries
The bounding region will include every entity and all its dependent entities.
This also means some entities outside the boundary will be inlcuded, as long as
they have any reference to entities within the boundary.
No geometry clipping is taking place, entities are exported as-is and can extend
hundreds of kilometers beyond the boundary if that is a continuous entity
(eg a long river).

### Unsorted ids

The output ids are streamed in the order they come from the database, and for spped reasons
this is unsorted. If you need a sorted `.osm`, use
[osmium](https://osmcode.org/osmium-tool/) :


`osmium sort in.osm.bz2  --output=out.osm.bz2`


### Output may change
This is experimental, data output may change to work better with other `.osm`-accepting tools.


### Errors
Currently, uncompressed `.osm` output files bigger than about a gigabyte error out
when used by `OsmAndMapCreator`.

### Unreliable output
The output data may have some missing attributes, because the database import
is not a lossless operation: importing a `.osm.pbf` to database, then exporting
it to `.osm` and then converting it back to `.osm.pbf` will produce a **different** file.


The data from the database is straightforwardly reinterpreted to mean key=value
osm tags. This is not strictly the case, for example the cycleway and bicycle
tags. Currently no back-transformations, to compensate for lua transforms,
are performed on the data.

Installation
---

* you need the `get_lonlat` utility, for that you need to have compiled
libosm2pgsql.a ([instructions](https://github.com/osm2pgsql-dev/osm2pgsql#building))
  - then copy `get_lonlat.cpp` to `osm2pgsql/src/get_lonlat.cpp`
  - and in the `osm2pgsql/build/` directory, after having run `make` :
  - `g++ ../src/get_lonlat.cpp ../src/node-persistent-cache.cpp
-I ../contrib/fmt/include/ ../build/src/libosm2pgsql.a -Wall -g -o get_lonlat`
  - Test: `./get_lonlat /path/to/planet.bin.nodes <<< 3546766428`
    * Should output `39.188565;49.885284;3546766428` if that node was covered by database import
* run `python3 -u pgsql2osm.py /path/to/get_lonlat /path/to/planet.bin.nodes [...]`


_Note_ : the `-u` option enables python unbuffered I/O, for the live progress
reports as percentages

Implementation details
--

* To generate `regions.csv`, run in a planet database:


`psql -d gis -p 5432 --csv -c "select -osm_id as osm_id,coalesce(tags->'name:en',name) as name,
    tags->'ISO3166-1' as iso_country,tags->'border_type' as border_type,
    tags->'ISO3166-2' as iso_subcountry,admin_level,tags->'wikipedia' as wikipedia
  from planet_osm_polygon where osm_id<0 and admin_level in ('1','2','3','4')
    and boundary='administrative'" > regions.csv`


  - _Note_ : only admin_level<=4 is loaded into `regions.csv`.
Use a search on [openstreetmap.org](openstreetmap.org) for smaller regions.
And then extract the relation id from the url:
[https://www.openstreetmap.org/relation/\<N\>](https://www.openstreetmap.org/relation/51701)
eg Switzerland

