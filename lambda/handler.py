import os
import sys
import logging
import datetime
import json

# add path to included packages
path = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.join(path, 'lib/python2.7/site-packages'))

import boto3
import grequests

from utilities import util, geoprocessing, serializers, gpkg_etl, combine_fires_s3


# https://stackoverflow.com/a/2588054/4355916
root = logging.getLogger()
if root.handlers:
    for handler in root.handlers:
        root.removeHandler(handler)

logging.basicConfig(format='%(asctime)s %(message)s',level=logging.ERROR)

client = boto3.client('lambda')


def fire_analysis(event, context):

    geom = util.get_shapely_geom(event)
    tile_id = event['queryStringParameters']['tile_id']
    fire_type = event['queryStringParameters']['fire_type']
    period = event['queryStringParameters']['period']

    if fire_type == 'all':
        fire_type = ['VIIRS', 'MODIS']
    else:
        fire_type = [fire_type.upper()]

    date_list = geoprocessing.point_stats(geom, tile_id, fire_type, period) # looks like {'2016-05-09': 15, '2016-05-10': 200, '2016:-5-11': 52}

    # makes json formatted info of tile_id: date list
    return serializers.serialize_fire_analysis(date_list, tile_id) # {'10N_010E': {'2016-05-09': 15, '2016-05-10': 200, '2016:-5-11': 52}}


def fire_alerts(event, context):

    try:
        geom = util.get_shapely_geom(event)
    except ValueError, e:
        return serializers.api_error(str(e))

    area_ha = util.get_polygon_area(geom)
    payload = {'geojson': json.loads(event['body'])['geojson']}

    try:
        params = util.validate_params(event)
    except ValueError, e:
        return serializers.api_error(str(e))

    # send list of tiles to another enpoint called fire_analysis(geom, tile)
    url = 'https://u81la7we82.execute-api.us-east-1.amazonaws.com/dev/fire-analysis'
    request_list = []

    # get list of tiles that intersect the aoi
    tiles = geoprocessing.find_tiles(geom)

    # add specific analysis type for each request
    for tile in tiles:
        new_params = params.copy()
        new_params['tile_id'] = tile

        request_list.append(grequests.post(url, json=payload, params=new_params))

    # execute these requests in parallel
    response_list = grequests.map(request_list, size=len(tiles))

    # merged_date_list looks like {datetime.datetime(2016, 6, 3, 0, 0): 12, datetime.datetime(2016, 4, 4, 0, 0): 14
    try:
        merged_date_list = geoprocessing.merge_dates(response_list, tiles)
    except KeyError, e:
        return serializers.api_error(str(e))

     # aggregate by
    resp_dict = geoprocessing.create_resp_dict(merged_date_list)

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
    aoi ={"type":"FeatureCollection","features":[{"type":"Feature","properties":{},"geometry":{"type":"Polygon","coordinates":[[[117.164337416055,-0.146001213786356],[117.155703106841,-0.057749439573639],[117.130133243555,0.027110960960791],[117.088610223703,0.105318959360494],[117.032729390131,0.173868987816331],[116.9646379261,0.230126442445014],[116.886952514797,0.271929022470392],[116.802658856842,0.297669979066676],[116.714996872182,0.306360018259741],[116.62733601195,0.297665417784521],[116.543045531146,0.271920848997643],[116.465364792161,0.230116400643847],[116.397278664419,0.173859331577133],[116.341402857481,0.105312081565839],[116.299883592981,0.027108993740147],[116.274315418049,-0.057744991082702],[116.265680230058,-0.145989746845834],[116.274309760144,-0.234235059943345],[116.299872885489,-0.319090657019565],[116.341388234208,-0.397296240890491],[116.397261631207,-0.465846611406709],[116.465347025389,-0.522107112718315],[116.543028657773,-0.563914980303813],[116.627321403215,-0.589662660364852],[116.714985480799,-0.598359835271131],[116.80265112075,-0.589671696300947],[116.886948340271,-0.56393193102085],[116.964636750894,-0.522129896874692],[117.032730315149,-0.465872485244865],[117.088612191345,-0.397322188160817],[117.130135233987,-0.319113812726435],[117.155704320927,-0.234253104759971],[117.164337416055,-0.146001213786356]]]}}]}
    # why this crazy structure? Oh lambda . . . sometimes I wonder
    fire_csv = 's3://gfw2-data/alerts-tsv/temp/fires-temp-10.csv'
    fire_data = [{'lat': '53.01', 'lon': '127.24700000000001', 'fire_type': 'MODIS', 'fire_date': '2016-04-30'}]
    event = {
            'body': json.dumps({'geojson': aoi, 'fire_data': fire_data, 'fire_csv': fire_csv}),
            #'body': {'fire_data': fire_data},
            'queryStringParameters': {'aggregate_by':'day', 'layer': 'glad', 'aggregate_values': 'true', 'tile_id': '00N_116E', 'fire-type': 'all', 'period': '2017-04-01,2018-02-02'}
            }

    print fire_alerts(event, None)
    #print validate_layer_extent(event, None) 
