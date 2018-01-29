from shapely.geometry import shape, Polygon
import fiona
import datetime
from util import grouped_and_to_rows


def find_tiles(geom):

    tiles = 's3://palm-risk-poc/data/fires/index.geojson'
    int_tiles = []

    with fiona.open(tiles, 'r', 'GeoJSON') as tiles:
        for tile in tiles:
            if shape(tile['geometry']).intersects(geom):
                tile_dict = tile['properties']
                tile_name = tile_dict['location'][-12:].replace(".tif", "")

                int_tiles.append(tile_name)

    return int_tiles

def point_stats(geom, tile_id, fire_type):
    # returns fire points within aoi within tile
    intersect_list = []

    with fiona.open('s3://palm-risk-poc/data/fires/{}/data.vrt'.format(tile_id), layer='data') as src:
        for pt in src:
            if pt['properties']['fire_type'] in fire_type and shape(pt['geometry']).intersects(geom): # fire_type should be VIIRS or MODIS
                fire_date = pt['properties']['fire_date']
                intersect_list.append(fire_date) ## need to include modis/veers too
    # intersect list looks like: [u'2016-05-09', u'2016-05-13', u'2016-06-03', u'2016-05-07', u'2016-05-07']

    # create dictionary of unique dates and how many fire points on that date
    date_counts = {}
    for d in intersect_list:
        try:
            date_counts[d] += 1
        except:
            date_counts[d] = 1

    # looks like {'2016-05-09': 15, '2016-05-13':20}
    return date_counts


def merge_dates(response_list, tile_id_list):
    # from format of [{"10N_110W": {"2016-06-06": 2, ...
    # get just {"2016-06-06": 2, } etc
    merged_dates = {}
    comb_dict = dict(pair for d in response_list for pair in d.items()) # dict with tileid: {date: count, date:count}

    for tile_id in tile_id_list:

        for alert_date_str, alert_count in comb_dict[tile_id].iteritems():
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
                 'year': grouped_and_to_rows([x.year for x in k], v, 'year'),
                 # month --> quarter calc: https://stackoverflow.com/questions/1406131
                 'quarter': grouped_and_to_rows([(x.year, (x.month-1)//3 + 1) for x in k], v, 'quarter'),
                 'month':  grouped_and_to_rows([(x.year, x.month) for x in k], v, 'month'),
                 'week': grouped_and_to_rows([(x.year, x.isocalendar()[1]) for x in k], v, 'week')
                }

    return resp_dict
