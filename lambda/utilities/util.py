import json
import datetime
from collections import defaultdict
from functools import partial
from shapely.ops import transform
import pyproj

from shapely.geometry import shape



def get_shapely_geom(event):

    try:
        geojson = json.loads(event['body'])['geojson']
    except:
        raise ValueError('No geojson key in body')

    if not isinstance(geojson, dict):
        raise ValueError('Unable to decode input geojson')

    if not geojson.get('features'):
        raise ValueError('No features in geojson')

    if len(geojson['features']) > 1:
        raise ValueError('Currently accepting only 1 feature at a time')

    # grab the actual geometry-- that's the level on which shapely operates
    try:
        aoi_geom = shape(geojson['features'][0]['geometry'])
    except:
        raise ValueError('Unable to decode input geojson')

    if 'Polygon' not in aoi_geom.type:
        raise ValueError('Geometry type must be polygon or multipolygon')

    return aoi_geom


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

def get_polygon_area(geom):
    # source: https://gis.stackexchange.com/a/166421/30899

    geom_area = transform(
        partial(
            pyproj.transform,
            pyproj.Proj(init='EPSG:4326'),
            pyproj.Proj(
                proj='aea',
                lat1=geom.bounds[1],
                lat2=geom.bounds[3])),
        geom)

    # return area in ha
    return geom_area.area / 10000.
