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
* database was imported with `osm2pgsql` and
  - has `planet_osm_ways` and `planet_osm_rels` tables,
  - has the standard rendering `planet_osm_polygon`, `planet_osm_line` and `planet_osm_point` tables
* access to the nodes cache binary file (readwrite for now)

Features
---
* XML streaming output, allows to store compressed .osm.bz2 on disk. example:
`pgsql2osm.py 'user=username dbname=gis' iso:fr -|bzip2 > France.osm.bz2`
* Automatic detection of the middle database format (legacy text[] or new jsonb, available in dbs created by osm2pgsql>=1.9)


Installation
---
* you need the `get_lonlat` utility, for that you need to have compiled libosm2pgsql.a ([instructions])
* write the `get_lonlat` path in `pgsql2osm.py`, line 
* run python3 pgsql2osm.py
