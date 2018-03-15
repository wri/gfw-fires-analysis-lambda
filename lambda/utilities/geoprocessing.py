import datetime
import subprocess

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

    
def point_stats(geom, tile_id, fire_type_list, period):
    # returns fire points within aoi within tile

    date_counts = {}

    local_gpkg = gpkg_etl.download_gpkg(tile_id)

    start_date, end_date = util.period_to_dates(period)

    with fiona.open(local_gpkg, layer='data') as src:
        for pt in src.filter(bbox=geom.bounds):
            fire_type = pt['properties']['fire_type']
            fire_geom = shape(pt['geometry'])
            fire_date = datetime.datetime.strptime(pt['properties']['fire_date'], '%Y-%m-%d').date()

            if fire_type in fire_type_list and (end_date >= fire_date >= start_date) and fire_geom.intersects(geom):
                fire_date = pt['properties']['fire_date']

                try:
                    date_counts[fire_date] += 1
                except KeyError:
                    date_counts[fire_date] = 1

    # looks like {'2016-05-09': 15, '2016-05-13':20}
    return date_counts


def merge_dates(response_list, tile_id_list):
    # from format of [{"10N_110W": {"2016-06-06": 2, ...
    # get just {"2016-06-06": 2, } etc

    merged_dates = {}

    comb_dict = dict(pair for d in response_list for pair in d.json().items()) # dict with tileid: {date: count, date:count}

    for tile_id in tile_id_list:
        try:
            date_dict = comb_dict[tile_id]
        except KeyError:
            raise KeyError('No response found for tile id {}'.format(tile_id))

        for alert_date_str, alert_count in date_dict.iteritems():
            alert_date = datetime.datetime.strptime(alert_date_str, "%Y-%m-%d")

            try:
                merged_dates[alert_date] += alert_count
            except KeyError:
                merged_dates[alert_date] = alert_count

    return merged_dates


def create_resp_dict(date_dict):
    k = date_dict.keys() # alert date = datetime.datetime(2015, 6, 4, 0, 0)
    v = date_dict.values() # count

    resp_dict = {
                 'year': util.grouped_and_to_rows([x.year for x in k], v, 'year'),
                 # month --> quarter calc: https://stackoverflow.com/questions/1406131
                 'quarter': util.grouped_and_to_rows([(x.year, (x.month-1)//3 + 1) for x in k], v, 'quarter'),
                 'month':  util.grouped_and_to_rows([(x.year, x.month) for x in k], v, 'month'),
                 'week': util.grouped_and_to_rows([(x.year, x.isocalendar()[1]) for x in k], v, 'week'),
                 'day': util.grouped_and_to_rows([(x.year, x.strftime('%Y-%m-%d')) for x in k], v, 'day')
                }

    return resp_dict
