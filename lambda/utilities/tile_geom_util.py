import math
from shapely.geometry import Polygon, mapping
import json

def get_nearest_quarter(input_val, dir):

    mult_four = input_val * 4
    if dir == 'down':
        round_down = math.floor(mult_four)
    if dir == 'up':
        round_down = math.ceil(mult_four)
    return round_down / 4


def tile_geom(geom):
    # https://gis.stackexchange.com/a/276901
    minx, miny, maxx, maxy = geom.bounds

    gridSize = 0.25

    minx = get_nearest_quarter(minx, 'down')
    miny = get_nearest_quarter(miny, 'down')
    maxx = get_nearest_quarter(maxx, 'up')
    maxy = get_nearest_quarter(maxy, 'up')

    height = int((maxx - minx) / gridSize)
    width = int((maxy - miny) / gridSize)

    ringXleftOrigin = minx
    ringXrightOrigin = minx + gridSize

    ringYtopOrigin = maxy
    ringYbottomOrigin = maxy - gridSize

    # start feature collection
    out_geojson = {"type": "FeatureCollection", "features":[]}

    # create a template for each feature
    feature_template = {"type": "Feature", "properties": {}, "geometry": {}}

    for i in range(1, height + 1):
        ringYtop = ringYtopOrigin
        ringYbottom =ringYbottomOrigin
        for j in range(1, width + 1):
            polygon = Polygon([(ringXleftOrigin, ringYtop), (ringXrightOrigin, ringYtop), (ringXrightOrigin, ringYbottom), (ringXleftOrigin, ringYbottom)])

            if polygon.intersects(geom):
                # create a copy of the template
                feature = feature_template.copy()

                # add the square geometry to the geometry attribute-
                # use mapping to convert shapely object to geojson-like dictionary
                feat_id = '{}_{}'.format(ringYtop, ringXleftOrigin)
                feature['geometry'] = mapping(polygon.intersection(geom))
                feature['properties'] = {'id': feat_id}
                out_geojson['features'].append(feature)

            ringYtop = ringYtop - gridSize
            ringYbottom = ringYbottom - gridSize
        ringXleftOrigin = ringXleftOrigin + gridSize
        ringXrightOrigin = ringXrightOrigin + gridSize

    return json.dumps(out_geojson)
