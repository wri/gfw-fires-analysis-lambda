import csv
import datetime
from dateutil.relativedelta import relativedelta
from urlparse import urlparse
from collections import OrderedDict
from StringIO import StringIO

import boto3
import botocore
import numpy as np
import fiona

schema = {'geometry': 'Point', 'properties': [('fire_type', 'str'), ('fire_date', 'str')]}


def bulk_fires_to_tile(s3_path):

    s3 = boto3.resource('s3')

    parsed = urlparse(s3_path)
    bucket = parsed.netloc
    key = parsed.path.lstrip('/')

    obj = s3.Object(bucket, key)

    # https://stackoverflow.com/a/3305964/4355916
    f = StringIO(obj.get()['Body'].read().decode('utf-8'))
    csv_reader = csv.DictReader(f)

    tile_dict = {}

    for row in csv_reader:
        rounded_lat = np.ceil(float(row['lat'])).astype(int).astype(str)
        rounded_lon = np.floor(float(row['lon'])).astype(int).astype(str)

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


def update_geopackage(src_gpkg, fire_list):

    temp_gpkg = '/tmp/temp.gpkg'

    feature_template = {'geometry': {'type': 'Point', 'coordinates': ()},
                                     'type': 'Feature', 'properties': OrderedDict()}

    # get list of dates we're updating
    # don't want any overlap between dates we're updating an data in gpkg
    update_date_list = [datetime.datetime.strptime(x['fire_date'], '%Y-%m-%d') for x in fire_list]

    # also want to remove any fires > 1 year old, because they're out of the scope of this project
    one_year_ago = datetime.datetime.now().date() - relativedelta(years=1)

    # open source geopackage to get older fires data
    with fiona.open(src_gpkg) as src:

        # open new temporary output for writing
        with fiona.open(temp_gpkg, 'w', schema=schema, driver='GPKG', layer='data') as dst:

            # copy existing fires to new database
            for record in src:
                record_date = datetime.datetime.strptime(record['properties']['fire_date'], '%Y-%m-%d').date()
                if record_date >= one_year_ago and record_date not in update_date_list:
                    dst.write(record)

            # write new files based on the input data
            for new_fire in fire_list:
                new_feature = feature_template.copy()
                new_feature['geometry']['coordinates'] = (float(new_fire['lon']), float(new_fire['lat']))
                new_feature['properties']['fire_date'] = new_fire['fire_date']
                new_feature['properties']['fire_type'] = new_fire['fire_type']

                dst.write(new_feature)

    return temp_gpkg
