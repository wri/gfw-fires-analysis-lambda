import os
import sys
import logging
import datetime
import json

# add path to included packages
path = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.join(path, 'lib/python2.7/site-packages'))

import boto3

from utilities import util, geoprocessing, serializers, gpkg_etl, combine_fires_s3


# https://stackoverflow.com/a/2588054/4355916
root = logging.getLogger()
if root.handlers:
    for handler in root.handlers:
        root.removeHandler(handler)

logging.basicConfig(format='%(asctime)s %(message)s',level=logging.ERROR)

client = boto3.client('lambda', region_name='us-east-1')


def fire_alerts(event, context):

    try:
        geom = util.get_shapely_geom(event)
        params = util.validate_params(event)
    except ValueError, e:
        return serializers.api_error(str(e))

    period = event['queryStringParameters']['period']

    date_count_dict = geoprocessing.point_stats(geom, period)  # looks like {'2016-05-09': 15, '2016-05-10': 200}

    # aggregate by day, year, month, etc
    resp_dict = geoprocessing.create_resp_dict(date_count_dict)

    return serializers.serialize_fire_alerts(resp_dict, params)


def bulk_fire_upload(event, context):

    # will ultimately pull from s3 into memory
    s3_path = event['queryStringParameters']['fire_csv']

    # only update one fire type at a time
    fire_type = event['queryStringParameters']['fire_type']

    # read s3 path into memory, snap to tiles
    # then return {'11N_072E': [{'fire_date': '2015-01-01, 'fire_type': 'VIIRS'} ...]}
    tile_dict = gpkg_etl.bulk_fires_to_tile(s3_path)

    # invoke individual lambda functions to update data
    # for each GPKG stored
    for tile_id, fires_list in tile_dict.iteritems():

        # temporary - do this directly before async with lambda
        tile_event = {'queryStringParameters': {'tile_id': tile_id, 'fire_type': fire_type},
                      'body': {'fire_data': fires_list}}

        # limit for invoke payload - write fire list as JSON to s3 instead
        if sys.getsizeof(json.dumps(tile_event)) > 131000:
            out_s3_path = gpkg_etl.write_fire_tile_to_s3(fires_list, fire_type, tile_id)
            tile_event['body']['fire_data'] = out_s3_path

        client.invoke(FunctionName='fire-alerts-dev-update-fire-tile',
                      InvocationType='Event',
                      Payload=json.dumps(tile_event))

    return None


def update_fire_tile(event, context):

    tile_id = event['queryStringParameters']['tile_id']
    fire_type = event['queryStringParameters']['fire_type']
    fire_data = event['body']['fire_data']

    print tile_id

    # download current gpkg
    local_gpkg = gpkg_etl.download_gpkg(tile_id)

    # copy old 1x1 data into new GPKG, then append new data
    updated_gpkg = gpkg_etl.update_geopackage(local_gpkg, fire_data, fire_type)

    # overwrite GPKG on s3
    gpkg_etl.save_to_s3(tile_id, updated_gpkg)

    return None


def nightly_fires_update(event, context):

    # uses event['fire_type'] in case of direct invocation from cloudwatch cron job
    try:
        params = event['queryStringParameters']
        fire_type = params.get('fire_type').upper()
    except KeyError:
        fire_type = event['fire_type']
        params = {}

    today = datetime.datetime.now()
    yesterday = (today - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    fire_date = params.get('fire_date', yesterday)

    combined_s3_file = combine_fires_s3.combine(fire_type, fire_date)

    invoke_event = {'queryStringParameters': {'fire_csv': combined_s3_file, 'fire_type': fire_type}}
    client.invoke(FunctionName='fire-alerts-dev-bulk-fire-upload', InvocationType='Event', Payload=json.dumps(invoke_event))

    return None


def validate_layer_extent(event, context):

    try:
        layer, geom = util.validate_extent_params(event)
    except ValueError, e:
        return serializers.api_error(str(e))

    geom_intersects = geoprocessing.check_layer_coverage(layer, geom)

    return serializers.serialize_layer_extent(layer, geom_intersects)


if __name__ == '__main__':

    with open('test_geom.geojson') as thefile:
        aoi = json.load(thefile)
    # aoi = {"type":"FeatureCollection","features":[{"type":"Feature","properties":{},"geometry":{"type":"Polygon","coordinates":[[[22.269287109374996,3.5298694189563014],[21.665039062499996,2.8772079526533365],[22.483520507812496,2.701635047944533],[22.7362060546875,2.871721700059574],[22.774658203125,3.299566301217904],[22.74169921875,3.568247821628616],[22.664794921874996,3.5956599859799567],[22.269287109374996,3.5298694189563014]]]}}]}

    # why this crazy structure? Oh lambda . . . sometimes I wonder
    fire_csv = 's3://gfw2-data/alerts-tsv/temp/fires-temp-10.csv'
    fire_data = [{'lat': '53.01', 'lon': '127.24700000000001', 'fire_type': 'MODIS', 'fire_date': '2016-04-30'}]
    event = {
            'body': json.dumps({'geojson': aoi, 'fire_data': fire_data, 'fire_csv': fire_csv}),
            'queryStringParameters': {'aggregate_by':'day', 'layer': 'glad', 'aggregate_values': 'true', 'fire_type': 'all', 'period': '2017-06-25,2018-07-02'}
            }

    print fire_alerts(event, None)
