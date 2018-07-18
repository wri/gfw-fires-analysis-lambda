import os
import sys
import logging
import json

# add path to included packages
path = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.join(path, 'lib/python2.7/site-packages'))

import boto3

from utilities import util, geoprocessing, serializers, gpkg_etl

# https://stackoverflow.com/a/2588054/4355916
root = logging.getLogger()
if root.handlers:
    for handler in root.handlers:
        root.removeHandler(handler)

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.ERROR)

client = boto3.client('lambda', region_name='us-east-1')


def fire_alerts(event, context):

    # set period
    # period = util.set_period()

    try:
        geom = util.get_shapely_geom(event)
        params = util.validate_params(event)
    except ValueError, e:
        return serializers.api_error(str(e))

    period = event['queryStringParameters']['period']

    date_count_dict = geoprocessing.point_stats(geom, period)  # {datetime.date(2018, 7, 10): 4392, datetime.date(2

    # put list of {date: count} into list with keys, values
    resp_dict = geoprocessing.create_resp_dict(date_count_dict)

    return serializers.serialize_fire_alerts(resp_dict, params)


def fires_update(event, context):

    # capture newly created csv so we can read it in
    s3_key_path = event['Records'][0]['s3']['object']['key']  #alerts-tsv/fires/temp/es_VIIRS_new_fires_2018-07-09-16-15.csv
    bucket_name = event['Records'][0]['s3']['bucket']['name']

    new_fires_s3 = 's3://{0}/{1}'.format(bucket_name, s3_key_path)

    gpkg_etl.update_geopackage(new_fires_s3)

    return None


def fires_export(event, context):

    # writes formatted csv to palm-risk-poc/fires-export when new VIIRS added

    # capture newly created csv so we can read it in
    s3_key_path = event['Records'][0]['s3']['object']['key']  #alerts-tsv/fires/temp/es_VIIRS_new_fires_2018-07-09-16-15.csv
    bucket_name = event['Records'][0]['s3']['bucket']['name']

    new_fires_s3 = 's3://{0}/{1}'.format(bucket_name, s3_key_path)

    fire_type = new_fires_s3.split('/')[-2]

    # if updated file is VIIRS, also update the geopackage. Invoke the fires_update event
    if fire_type == 'VIIRS':
        print "updating viirs data"
        client.invoke(
            FunctionName='fire-alerts-dev-fires-update',
            InvocationType='Event',
            Payload=json.dumps(event))

    print "exporting new VIIRS and MODIS csv for hadoop"
    util.write_fires_export(new_fires_s3)


def validate_layer_extent(event, context):

    try:
        layer, geom = util.validate_extent_params(event)
    except ValueError, e:
        return serializers.api_error(str(e))

    geom_intersects = geoprocessing.check_layer_coverage(layer, geom)

    return serializers.serialize_layer_extent(layer, geom_intersects)


if __name__ == '__main__':

    aoi = {"type": "FeatureCollection", "name": "test_geom",
               "crs": {"type": "name", "properties": {"name": "urn:ogc:def:crs:OGC:1.3:CRS84"}}, "features": [
                {"type": "Feature", "properties": {"id": 1}, "geometry": {"type": "Polygon", "coordinates": [
                    [[19.607011190476182, -8.765827380952395], [25.652106428571422, -14.702001984126998],
                     [25.652106428571422, -14.702001984126998], [21.485892142857136, -19.848501984126997],
                     [14.406050873015866, -17.397787698412714], [14.215439761904756, -11.162081349206364],
                     [19.607011190476182, -8.765827380952395]]]}}]}

    event_fire_query = {
            'body': json.dumps({'geojson': aoi}),
            'queryStringParameters': {
                'aggregate_by': 'day',
                'aggregate_values': 'true'}
            }

    event_fires_export = {"Records":[
                {"s3":{
                    "bucket": {"name": "gfw2-data"},
                    "object": {"key": "fires/fires_for_elasticsearch/VIIRS/es_VIIRS_new_fires_2018-07-10-16-15.csv"}
                }
                }]}

    fires_export(event_fires_export, None)
