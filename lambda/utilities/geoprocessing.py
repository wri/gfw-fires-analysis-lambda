from shapely.geometry import shape, Polygon
import fiona
import datetime

from util import grouped_and_to_rows
from athena_query import FirePointsGenerator


def find_tiles(geom):

    tiles = 's3://palm-risk-poc/data/fires/index.geojson'
    int_tiles = []

    with fiona.open(tiles, 'r', 'GeoJSON') as tiles:
        for tile in tiles:
            if shape(tile['geometry']).intersects(geom):
                tile_dict = tile['properties']
                print tile_dict
                tile_name = tile_dict['ID']

                int_tiles.append(tile_name)

    return int_tiles


def point_stats(geom, tile_id, fire_type_list, period):

    generator = FirePointsGenerator(geom, tile_id, fire_type_list, period)
    date_counts = generator.generate()

    # looks like {'2016-05-09': 15, '2016-05-13':20}
    return date_counts


def merge_dates(response_list, tile_id_list):
    # from format of [{"10N_110W": {"2016-06-06": 2, ...
    # get just {"2016-06-06": 2, } etc

    merged_dates = {}

    comb_dict = dict(pair for d in response_list for pair in d.json().items()) # dict with tileid: {date: count, date:count}

    for tile_id in tile_id_list:
        print tile_id
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
