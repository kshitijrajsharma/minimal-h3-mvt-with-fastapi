# minimal-mvt-with-fastapi
build minimal vector tile endpoint for h3 table with fastapi

h3 indexes can be generated from https://github.com/kshitijrajsharma/raster-analysis-using-h3/blob/master/cog2h3.py 

## Install
```shell
pip install fastapi uvicorn asyncpg cachetools asyncache
```

## Setup Env variables
You can either place them in .env or export them as system env variables like following 

```shell
export DATABASE_URL="postgresql://postgres:postgres@localhost:5432/postgres"
export TILE_TABLE_NAME="yourtablename"
export TILE_TABLE_SRID="4326"
export TILE_TABLE_H3INX_COLUMN="h3_ix"
export TILE_TABLE_H3INX_RESOLUTION=8
export TILE_TABLE_ATTR_COLUMNS="cell_value"

## Run 
```shell
python main.py
```

## Fetch 
You can start fetching tiles like following
```url
http://localhost:8080/10/300/384.pbf
```