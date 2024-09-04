## Inspired from https://github.com/pramsey/minimal-mvt/blob/8b736e342ada89c5c2c9b1c77bfcbcfde7aa8d82/minimal-mvt.py
## Copyright @kshitijrajsharma2024

from fastapi import FastAPI, HTTPException, Body, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
import asyncpg
from asyncache import cached
from cachetools import TTLCache
from starlette.middleware.base import BaseHTTPMiddleware
from fastapi.staticfiles import StaticFiles
from starlette.requests import Request
import os
import json
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class CustomHeaderMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        if request.url.path.endswith(".pbf"):
            response.headers["Content-Type"] = "application/x-protobuf"
        return response

TILE_DIR = os.getenv('TILE_DIR', './tiles')

app.mount("/tiles", StaticFiles(directory=TILE_DIR), name="tiles")

# Add custom middleware to set headers
app.add_middleware(CustomHeaderMiddleware)
DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://kshitij:admin@localhost:5432/postgres"
)

TABLE = {
    'table': os.getenv('TILE_TABLE_NAME', 'flood2yr'),
    'srid': os.getenv('TILE_TABLE_SRID', '4326'),
    'h3inxColumn': os.getenv('TILE_TABLE_H3INX_COLUMN', 'h3_ix'),
    'h3inxRes': os.getenv('TILE_TABLE_H3INX_RESOLUTION', 10),
    'attrColumns': os.getenv('TILE_TABLE_ATTR_COLUMNS', 'cell_value')
}

# Create a cache with a maximum of 1000 items and a 1-hour TTL
cache = TTLCache(maxsize=500, ttl=1800)

async def get_db_pool():
    return await asyncpg.create_pool(DATABASE_URL)

@cached(cache)
async def get_tile(zoom: int, x: int, y: int, pool):
    async with pool.acquire() as conn:
        env = tile_to_envelope(zoom, x, y)
        sql = envelope_to_sql(env)
        return await conn.fetchval(sql)

def tile_to_envelope(zoom: int, x: int, y: int):
    world_merc_max = 20037508.3427892
    world_merc_min = -world_merc_max
    world_merc_size = world_merc_max - world_merc_min
    world_tile_size = 2 ** zoom
    tile_merc_size = world_merc_size / world_tile_size

    env = {
        'xmin': world_merc_min + tile_merc_size * x,
        'xmax': world_merc_min + tile_merc_size * (x + 1),
        'ymin': world_merc_max - tile_merc_size * (y + 1),
        'ymax': world_merc_max - tile_merc_size * y
    }
    return env


@app.post("/query")
async def execute_query(
    geometry: dict = Body(...),
    less_than: int = Body(...),
    greater_than: int = Body(...),
    pool = Depends(get_db_pool)
):
    try:
        geojson = json.dumps(geometry)

        query = f"""
        WITH t1 AS (
            SELECT *
            FROM flood2yr el
            WHERE h3_ix = ANY (
                get_h3_indexes(
                    ST_GeomFromGeoJSON('{geojson}'), 10
                )
            )
            AND cell_value > {greater_than}
            AND cell_value < {less_than}
        )
        SELECT count(*)
        FROM buildings bl
        JOIN t1 ON bl.h3_ix = t1.h3_ix;
        """

        async with pool.acquire() as conn:
            result = await conn.fetchval(query)

        return {"count": result}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def envelope_to_bounds_sql(env):
    DENSIFY_FACTOR = 4
    env['segSize'] = (env['xmax'] - env['xmin']) / DENSIFY_FACTOR
    sql_tmpl = 'ST_Segmentize(ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax}, 3857), {segSize})'
    return sql_tmpl.format(**env)

def envelope_to_sql(env):
    tbl = TABLE.copy()
    tbl['env'] = envelope_to_bounds_sql(env)
    sql_tmpl = """
        WITH
        bounds AS (
            SELECT {env} AS geom,
                   {env}::box2d AS b2d
        ),
        mvtgeom AS (
            SELECT ST_AsMVTGeom(ST_Transform(h3_cell_to_boundary_geometry(t.{h3inxColumn}), 3857), bounds.b2d) AS geom,
                   {attrColumns}
            FROM {table} t, bounds
            WHERE {h3inxColumn} = ANY (get_h3_indexes(ST_Transform(bounds.geom, {srid}),{h3inxRes}))
        )
        SELECT ST_AsMVT(mvtgeom.*) FROM mvtgeom
    """
    return sql_tmpl.format(**tbl)

@app.on_event("startup")
async def startup_event():
    app.state.pool = await get_db_pool()

@app.on_event("shutdown")
async def shutdown_event():
    await app.state.pool.close()

@app.get("/flood2yr/{zoom}/{x}/{y}.{format}")
async def get_mvt_tile(zoom: int, x: int, y: int, format: str):
    if format not in ['pbf', 'mvt']:
        raise HTTPException(status_code=400, detail="Invalid format. Use 'pbf' or 'mvt'.")

    tile_size = 2 ** zoom
    if x < 0 or y < 0 or x >= tile_size or y >= tile_size:
        raise HTTPException(status_code=400, detail="Invalid tile coordinates.")

    try:
        pbf = await get_tile(zoom, x, y, app.state.pool)
        return Response(content=pbf, media_type="application/vnd.mapbox-vector-tile")
    except Exception as e:
        # raise e
        raise HTTPException(status_code=500, detail=str(e))
    

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=80)