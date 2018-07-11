import json
import datetime
import subprocess
import os
from dateutil.relativedelta import relativedelta
from datetime import timedelta, datetime
from collections import defaultdict
from functools import partial
import csv

from shapely.ops import transform
from shapely.geometry import shape
import pyproj


def get_shapely_geom(event):

    try:
        geojson = json.loads(event['body'])['geojson']
    except:
        raise ValueError('No geojson key in body')

    if not isinstance(geojson, dict):
        raise ValueError('Unable to decode input geojson')

    if geojson['type'].lower() == 'featurecollection':

        if len(geojson['features']) > 1:
            raise ValueError('Currently accepting only 1 feature at a time')

        # grab the actual geometry-- that's the level on which shapely operates
        try:
            geom = shape(geojson['features'][0]['geometry'])
        except:
            raise ValueError('Unable to decode input geojson')

    else:
        if geojson.get('geometry'):
            try:
                geom = shape(geojson['geometry'])
            except:
                raise ValueError('Unable to decode input geojson')
        else:
            raise ValueError('GeoJSON input not formatted properly')

    if 'Polygon' not in geom.type:
        raise ValueError('Geometry type must be polygon or multipolygon')

    return geom


def validate_extent_params(event):

    params = event['queryStringParameters']

    if not params:
        params = {}

    # may add MODIS or VIIRS at some point
    valid_layers = ['glad']

    try:
        layer_name = params['layer'].lower()
    except KeyError:
        raise ValueError('Query parameter layer must included')

    if layer_name not in valid_layers:
        raise ValueError('Layer must be one of {}'.format(', '.join(valid_layers)))

    geom = get_shapely_geom(event) 

    return layer_name, geom


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


def validate_params(event):

    # get query parameters. if none supplied, set to empty dict, that way params.get doesn't throw an error
    params = event.get('queryStringParameters')
    if not params:
        params = {}

    today = datetime.now().date()

    # include last year - one day, because today's update will come at the end of the day
    # so if today is May 5 2018, want to include data from May 4 2017, because we
    # don't have data from May 5 2018 in our GPKGs yet
    last_8_days = today - relativedelta(days=8)
    default_period = last_8_days.strftime('%Y-%m-%d') + ',' + today.strftime('%Y-%m-%d')

    period = params.get('period', default_period)
    params['period'] = period

    try:
        check_dates(period, last_8_days)
    except ValueError, e:
        raise ValueError(e)

    # get agg values parameter. if not specified, set to false
    agg_values = params.get('aggregate_values', False)

    # if agg values is set to true in any of these forms, correct it to bool
    if agg_values in ['true', 'TRUE', 'True', True]:
        params['aggregate_values'] = True

    else:
        raise ValueError('aggregate_values must be set to true')

    # if user does not supply correct/spelling of agg_by params, raise error
    agg_by = params.get('aggregate_by')
    agg_by_options = ['year', 'quarter', 'month', 'week', 'day', 'all']
    if agg_by not in agg_by_options:
        raise ValueError('You must supply an aggregate_by param: {}'.format(', '.join(agg_by_options)))

    return params


def check_dates(period, last_8_days):

    try:
        start_date, end_date = period_to_dates(period)

    except ValueError:
        raise ValueError('period must be formatted as YYYY-mm-dd,YYYY-mm-dd')

    if start_date > end_date:
        raise ValueError('Start date must be <= end date')

    if start_date < last_8_days:
        raise ValueError('Start date must be more recent than 8 days ago')


def period_to_dates(period):

    start_date, end_date = period.split(',')
    start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
    end_date = datetime.strptime(end_date, '%Y-%m-%d').date()

    return start_date, end_date


def s3_download(path):
    cmd = ['aws', 's3', 'cp', path, '.']
    subprocess.check_call(cmd)
    file = path.split('/')[-1:]
    if os.path.exists(file):
        return file


def create_vrt(in_csv):
    lyr_name = in_csv.strip(".csv")
    lyr_name = lyr_name.split("/")[-1:][0]
    fires_vrt = '/tmp/fires.vrt'
    vrt_text = '''<OGRVRTDataSource>
                    <OGRVRTLayer name="{0}">
                    <SrcDataSource relativeToVRT="1">{0}.csv</SrcDataSource>
                    <GeometryType>wkbPoint</GeometryType>
                    <LayerSRS>WGS84</LayerSRS>
                    <GeometryField encoding="PointFromColumns" x="longitude" y="latitude"/>
                  </OGRVRTLayer>
                </OGRVRTDataSource>'''.format(lyr_name)

    with open(fires_vrt, 'w') as thefile:
        thefile.write(vrt_text)

    return fires_vrt


def fix_csv_date_lines(in_lines):
    date_10_days_ago = datetime.now() - timedelta(days=10)

    new_rows_list = []
    for line in in_lines:
        lat = line['latitude']
        long = line['longitude']

        fire_date = line['fire_datetime']
        formatted_date = datetime.strptime(fire_date, '%Y/%m/%d %H:%M:%S')

        if formatted_date >= date_10_days_ago:
            new_date = formatted_date.date().strftime('%Y-%m-%d')
            new_row = [lat, long, new_date]
            new_rows_list.append(new_row)

    fires_formatted = '/tmp/fires_formatted.csv'
    # fires_formatted = 'fires_formatted.csv'
    fires_formatted_date = open(fires_formatted, 'w')
    writer = csv.writer(fires_formatted_date)

    # write header
    writer.writerow(["latitude", "longitude", "fire_date"])

    # write all data
    writer.writerows(new_rows_list)

    fires_formatted_date.close()

    return fires_formatted


def fix_csv_date(in_csv):
    date_10_days_ago = datetime.now() - timedelta(days=10)

    new_rows_list= []
    fires = open(in_csv, 'r')
    reader = csv.reader(fires)
    next(reader, None)
    print "reading and fixing date"
    for row in reader:
        fire_date = row[12]
        formatted_date = datetime.strptime(fire_date, '%Y/%m/%d %H:%M:%S')

        if formatted_date >= date_10_days_ago:
            new_date = formatted_date.date().strftime('%Y-%m-%d')

            new_row = [row[0], row[1], new_date]
            new_rows_list.append(new_row)

    fires.close()

    fires_formatted = 'fires_formatted.csv'
    fires_formatted_date = open(fires_formatted, 'wb')
    writer = csv.writer(fires_formatted_date)
    writer.writerow(["latitude", "longitude", "fire_datetime"])
    writer.writerows(new_rows_list)

    fires_formatted_date.close()

    return fires_formatted
