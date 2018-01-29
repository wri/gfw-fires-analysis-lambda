import json
import datetime
from collections import defaultdict

from shapely.geometry import shape


def get_shapely_geom(event):

    print event
    geojson = json.loads(event['body'])['geojson']

    if len(geojson['features']) > 1:
        raise ValueError('Currently accepting only 1 feature at a time')

    # grab the actual geometry-- that's the level on which shapely operates
    return shape(geojson['features'][0]['geometry'])

def grouped_and_to_rows(keys, vals, agg_type):

    # source: https://jakevdp.github.io/blog/2017/03/22/group-by-from-scratch/
    count = defaultdict(int)
    for key, val in zip(keys, vals):
        count[key] += val
    grouped = dict(count)

    final_list = []

    for key, val in grouped.iteritems():

        if agg_type == 'year':
	    row = {agg_type: key}
        else:
            row = {'year': key[0], agg_type: key[1]}

        row['count'] = val
        final_list.append(row)

    return final_list


def clean_fire_type_input(fire_type):

    valid_fire_list = ['viirs', 'modis', 'all']

    if fire_type:
        if fire_type.lower() in valid_fire_list:
            return fire_type.lower()
        else:
            msg = 'For this batch service, fire_type must be in {}'.format(', '.join(valid_fire_list))
            raise ValueError(msg)

    else:
        return "all"



    if fire_type not in fire_options or valid_type != True:

        return gfw_api.api_error(msg)
