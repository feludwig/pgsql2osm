# pgsql2osm
Export .osm from a PostgreSQL openstreetmap database

Motivation
---

You have access to a PostgreSQL+PostGIS database which contains openstreetmap data imported by osm2pgsql for rendering maps. How can you generate an extract to use with other cartography tools ?
pgsql2osm.py is an experimental tool that can generate the XML in `.osm` format for given boundaries.
The main usecase I have for this is to generate an `.obf` file for the Android OsmAnd app, chained with `OsmAndMapCreator` for `osm` -> `obf`.

Requirements
---

* `python3`
  - `pip install psycopg2`
* database was imported with `osm2pgsql` and
  - has `planet_osm_ways` and `planet_osm_rels` tables,
  - has the standard rendering `planet_osm_polygon`, `planet_osm_line` and `planet_osm_point` tables
* access to the nodes cache binary file (readwrite for now)

Features
---

* XML streaming output, allows to store compressed .osm.bz2 on disk. example:
`pgsql2osm.py 'user=username dbname=gis' iso:fr -|bzip2 > France.osm.bz2`
* Attempt to lower RAM footprint with generators for database queries
* Automatic detection of the middle database format (legacy text[] or new jsonb, available in dbs created by osm2pgsql>=1.9)
* Bounds specifiable as either:
  - iso country or region code `--iso de-by` for Germany/Bayern
    `grep -i malaysia regions.csv` can show you regognized iso codes for Malaysia
  - osm relation id `--osm-rel-id N`, for any relation in the database. You can look at a relation at [https://www.openstreetmap.org/relation/<N>](https://www.openstreetmap.org/relation/51701) Switzerland
  - geojson input file `--geojson file.geojson`
  - bounding box `--box='<lon_from>,<lat_from>,<lon_to>,<lat_to>'`

Disclaimer
---
This is experimental, data output may change to work better with other .osm-accepting tools.
Currently, uncompressed .osm output files bigger than about a gigabyte error out with OsmAndMapCreator.

Installation
---

* you need the `get_lonlat` utility, for that you need to have compiled libosm2pgsql.a ([instructions](https://github.com/osm2pgsql-dev/osm2pgsql#building))
  - then copy `get_lonlat.cpp` to `osm2pgsql/src`
  - and in the `osm2pgsql/build/` directory, after having run `make`
  - `g++ ../src/get_lonlat.cpp ../src/node-persistent-cache.cpp -I ../contrib/fmt/include/ ../build/src/libosm2pgsql.a -Wall -g -o get_lonlat`
  - Test: `./get_lonlat /path/to/planet.bin.nodes <<< 3546766428`
      Should output `39.188565;49.885284;3546766428`
* run `python3 -u pgsql2osm.py /path/to/get_lonlat /path/to/planet.bin.nodes [...]`
  Note: the `-u` option enables python unbuffered io, for the live progress reports as percentages

Implementation details
---

* To generate regions.csv, run in a planet datagase:
  `psql -d gis -p 5432 --csv -c "select -osm_id as osm_id,coalesce(tags->'name:en',name) as name,tags->'ISO3166-1' as iso_country,tags->'border_type' as border_type,tags->'ISO3166-2' as iso_subcountry,admin_level,tags->'wikipedia' as wikipedia from planet_osm_polygon where osm_id<0 and admin_level IN ('1','2','3','4') and boundary='administrative'" > regions.csv`
