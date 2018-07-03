import datetime
import subprocess
import json

from shapely.geometry import shape, Polygon
import fiona
import boto3


# https://stackoverflow.com/a/31602136/4355916
from gevent import monkey
monkey.patch_all()

import util, gpkg_etl


def find_tiles(geom):

    tiles = 's3://palm-risk-poc/data/fires-one-by-one/index.geojson'
    int_tiles = []

    with fiona.open(tiles, 'r', 'GeoJSON') as tiles:
        for tile in tiles:
            if shape(tile['geometry']).intersects(geom):
                tile_dict = tile['properties']
                tile_name = tile_dict['ID']

                int_tiles.append(tile_name)

    return int_tiles


def check_layer_coverage(layer, geom):

    # need to read it in as bytes first because it's zipped on s3
    s3 = boto3.resource('s3')
    bucket = 'gfw2-data'
    key = 'forest_change/umd_landsat_alerts/zip/glad_coverage.zip'

    glad_obj = s3.Object(bucket, key)
    as_bytes = glad_obj.get()['Body'].read()

    geom_intersects = False

    with fiona.BytesCollection(as_bytes) as src:
        for record in src:
            if shape(record['geometry']).intersects(geom):
                geom_intersects = True
                break

    return geom_intersects


def point_stats(geom, period, local_gpkg=None):

    # returns fire points within aoi
    date_counts = {}

    if not local_gpkg:
        local_gpkg = gpkg_etl.download_gpkg()

    start_date, end_date = util.period_to_dates(period)

    with fiona.open(local_gpkg, layer='data') as src:

        for pt in src.filter(bbox=geom.bounds):
            fire_geom = shape(pt['geometry'])
            fire_date = datetime.datetime.strptime(pt['properties']['fire_date'], '%Y-%m-%d').date()

            if (end_date >= fire_date >= start_date) and fire_geom.intersects(geom):

                try:
                    date_counts[fire_date] += 1
                except KeyError:
                    date_counts[fire_date] = 1

    # looks like {2016-05-09: 15, 2016-05-13:20}
    print "\n*********DATE COUNTS: {}".format(date_counts)
    return date_counts


def create_resp_dict(date_dict):

    alert_date = date_dict.keys() # alert date = datetime.datetime(2015, 6, 4, 0, 0)
    alert_count = date_dict.values() # count

    resp_dict = {
                 'year': util.grouped_and_to_rows([x.year for x in alert_date], alert_count, 'year'),
                 # month --> quarter calc: https://stackoverflow.com/questions/1406131
                 'quarter': util.grouped_and_to_rows([(x.year, (x.month-1)//3 + 1) for x in alert_date], alert_count, 'quarter'),
                 'month':  util.grouped_and_to_rows([(x.year, x.month) for x in alert_date], alert_count, 'month'),
                 'week': util.grouped_and_to_rows([(x.year, x.isocalendar()[1]) for x in alert_date], alert_count, 'week'),
                 'day': util.grouped_and_to_rows([(x.year, x.strftime('%Y-%m-%d')) for x in alert_date], alert_count, 'day')
                }

    return resp_dict
