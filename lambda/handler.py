import os
import sys
import logging
import json

# add path to included packages
path = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.join(path, 'lib/python2.7/site-packages'))

import fiona
import shapely
import grequests

from utilities import util, geoprocessing, serializers, geo_utils, serializers


# set up logger
logger = logging.getLogger(__file__)
logger.setLevel(logging.DEBUG)
# commented out to avoid duplicate logs in lambda
# logger.addHandler(logging.StreamHandler())

def fire_analysis(event, context):

    geom = util.get_shapely_geom(event)
    tile_id = event['queryStringParameters']['tile_id']
    fire_type = event['queryStringParameters']['fire_type']

    if fire_type == 'all':
        fire_type = ['VIIRS', 'MODIS']
    else:
        fire_type = [fire_type.upper()]

    date_list = geoprocessing.point_stats(geom, tile_id, fire_type) # looks like {'2016-05-09': 15, '2016-05-10': 200, '2016:-5-11': 52}

    # makes json formatted info of tile_id: date list
    return serializers.serialize_fire_analysis(date_list, tile_id) # {'10N_010E': {'2016-05-09': 15, '2016-05-10': 200, '2016:-5-11': 52}}


def fire_alerts(event, context):

    geom = util.get_shapely_geom(event)
    area_ha = geo_utils.get_polygon_area(geom)

    payload = {'geojson': json.loads(event['body'])['geojson']}

    params = event.get('queryStringParameters')
    if not params:
        params = {}

    agg_by = params.get('aggregate_by')
    fire_type = params.get('fire_type')

    try:
        fire_type = util.clean_fire_type_input(fire_type)
    except ValueError, e:
        return serializers.api_error(e)

    # send list of tiles to another enpoint called fire_analysis(geom, tile)
    url = 'https://u81la7we82.execute-api.us-east-1.amazonaws.com/dev/fire-analysis'
    request_list = []

    # get list of tiles that intersect the aoi
    tiles = geoprocessing.find_tiles(geom)

    # add specific analysis type for each request
    for tile in tiles:

        new_params = params.copy()
        new_params['tile_id'] = tile
        new_params['fire_type'] = fire_type

        request_list.append(grequests.post(url, json=payload, params=new_params))

    # execute these requests in parallel
    response_list = grequests.map(request_list, size=len(tiles))

    # merged_date_list looks like {datetime.datetime(2016, 6, 3, 0, 0): 12, datetime.datetime(2016, 4, 4, 0, 0): 14
    merged_date_list = geoprocessing.merge_dates(response_list, tiles)

     # aggregate by
    resp_dict = geoprocessing.create_resp_dict(merged_date_list)

    return serializers.serialize_fire_alerts(resp_dict, agg_by)


if __name__ == '__main__':
    aoi ={"type":"FeatureCollection","features":[{"type":"Feature","properties":{},"geometry":{"type":"Polygon","coordinates":[[[117.164337416055,-0.146001213786356],[117.155703106841,-0.057749439573639],[117.130133243555,0.027110960960791],[117.088610223703,0.105318959360494],[117.032729390131,0.173868987816331],[116.9646379261,0.230126442445014],[116.886952514797,0.271929022470392],[116.802658856842,0.297669979066676],[116.714996872182,0.306360018259741],[116.62733601195,0.297665417784521],[116.543045531146,0.271920848997643],[116.465364792161,0.230116400643847],[116.397278664419,0.173859331577133],[116.341402857481,0.105312081565839],[116.299883592981,0.027108993740147],[116.274315418049,-0.057744991082702],[116.265680230058,-0.145989746845834],[116.274309760144,-0.234235059943345],[116.299872885489,-0.319090657019565],[116.341388234208,-0.397296240890491],[116.397261631207,-0.465846611406709],[116.465347025389,-0.522107112718315],[116.543028657773,-0.563914980303813],[116.627321403215,-0.589662660364852],[116.714985480799,-0.598359835271131],[116.80265112075,-0.589671696300947],[116.886948340271,-0.56393193102085],[116.964636750894,-0.522129896874692],[117.032730315149,-0.465872485244865],[117.088612191345,-0.397322188160817],[117.130135233987,-0.319113812726435],[117.155704320927,-0.234253104759971],[117.164337416055,-0.146001213786356]]]}}]}

    # why this crazy structure? Oh lambda . . . sometimes I wonder
    event = {
             'body': json.dumps({'geojson': aoi}),
             'queryStringParameters': {'aggregate_by':'year', 'aggregate_values': 'True', 'tile_id': '00N_110E', 'fire_type': 'all'}
            }

    fire_alerts(event, None)
