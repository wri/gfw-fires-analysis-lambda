import csv
import sqlite3
import datetime
import json
from dateutil.relativedelta import relativedelta
from urlparse import urlparse
from collections import OrderedDict
from StringIO import StringIO

import boto3
import botocore
import numpy as np
import fiona

schema = {'geometry': 'Point', 'properties': [('fire_type', 'str'), ('fire_date', 'str')]}


def s3_to_csv_reader(s3_path):

    s3 = boto3.resource('s3')

    parsed = urlparse(s3_path)
    bucket = parsed.netloc
    key = parsed.path.lstrip('/')

    obj = s3.Object(bucket, key)

    # https://stackoverflow.com/a/3305964/4355916
    as_str = StringIO(obj.get()['Body'].read().decode('utf-8'))

    # return list of lines
    return as_str.getvalue().splitlines()


def bulk_fires_to_tile(s3_path):

    csv_reader = csv.DictReader(s3_to_csv_reader(s3_path))

    tile_dict = {}

    for row in csv_reader:
        rounded_lat = np.ceil(float(row['LATITUDE'])).astype(int).astype(str)
        rounded_lon = np.floor(float(row['LONGITUDE'])).astype(int).astype(str)

        tile_id = rounded_lat + '_' + rounded_lon

        # format it to Hansen tile ID spec -- 00N_000E, etc
        tile_id = clean_tile_id(tile_id)

        try:
            tile_dict[tile_id].append(row)
        except KeyError:
            tile_dict[tile_id] = [row]

    return tile_dict


def get_bucket():

    s3 = boto3.resource('s3')
    bucket = s3.Bucket('palm-risk-poc')

    return bucket


def download_gpkg(tile_id):

    # weirdly this seems 2x - 3x as fast as reading directly from s3
    # maybe spatial index isn't used when reading from s3?
    bucket = get_bucket()

    gpkg_src = 'data/fires-one-by-one/{}/data.gpkg'.format(tile_id)
    local_gpkg = '/tmp/data.gpkg'

    try:
        bucket.download_file(gpkg_src, local_gpkg)

    # if it doesn't exist create a dummy geopackage
    except botocore.exceptions.ClientError:
        with fiona.open(local_gpkg, 'w', layer='data', driver='GPKG', schema=schema) as f:
            pass

    return local_gpkg


def save_to_s3(tile_id, local_gpkg):

    bucket = get_bucket()
    out_gpkg = 'data/fires-one-by-one/{}/data.gpkg'.format(tile_id)

    bucket.upload_file(local_gpkg, out_gpkg)


def clean_tile_id(tile_id):
    lat, lon = tile_id.split('_')

    if '-' in lat:
        lat = lat[1:] + 'S'
    else:
        lat += 'N'

    if '-' in lon:
        lon = lon[1:] + 'W'
    else:
        lon += 'E'

    lat = lat.zfill(3)
    lon = lon.zfill(4)

    return '_'.join([lat, lon])


def update_geopackage(src_gpkg, fire_list, fire_type):

    feature_template = {'geometry': {'type': 'Point', 'coordinates': ()},
                                     'type': 'Feature', 'properties': OrderedDict()}

    # if we have a lot of fires to update, fire_list may be a string pointing
    # to an s3 object (s3://palm-risk-poc/ . . . 
    if isinstance(fire_list, basestring):
        fire_list = [x for x in csv.DictReader(s3_to_csv_reader(fire_list))]

    # open source geopackage to write new data
    with fiona.open(src_gpkg, 'a', schema=schema, driver='GPKG', layer='data') as src:

        # write new files based on the input data
        for new_fire in fire_list:
            new_feature = feature_template.copy()
            new_feature['geometry']['coordinates'] = (float(new_fire['LONGITUDE']), float(new_fire['LATITUDE']))
            new_feature['properties']['fire_type'] = fire_type

            # read in timtestamp as string, convert to date, then format to match GPKG standard
            formatted_date = datetime.datetime.strptime(new_fire['ACQ_DATE'], '%m/%d/%Y %H:%M:%S').date().strftime('%Y-%m-%d')
            new_feature['properties']['fire_date'] = formatted_date
            src.write(new_feature)

    # after we've added our new data, delete any duplicate fires
    # based on lat/lon/fire_type/fire_date
    delete_duplicate_fires(src_gpkg)

    return src_gpkg


def delete_duplicate_fires(src_gpkg):

    # connect to GPKG and delete any duplicate data 
    conn = sqlite3.connect(src_gpkg)
    cur = conn.cursor()

    sql_str = ("DELETE FROM data " 
               "WHERE rowid NOT IN ( "
               "SELECT min(rowid) "
               "FROM data "
               "GROUP BY geom, fire_type, fire_date);")
    cur.execute(sql_str)
       
    conn.commit()
    conn.close()


def write_fire_tile_to_s3(fire_list, fire_type, tile_id):
    
    date_fmt = datetime.datetime.today().date().strftime('%Y%m%d')
    out_s3_path = 'temp/fires/{}/{}/{}.json'.format(fire_type, date_fmt, tile_id)

    s3 = boto3.client('s3')
    bucket_name = 'palm-risk-poc'
    s3.put_object(Bucket=bucket_name, Key=out_s3_path, Body=json.dumps(fire_list))

    return 's3://{}/{}'.format(bucket_name, out_s3_path)

