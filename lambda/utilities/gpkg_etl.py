import csv
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


def s3_to_object(s3_path):

    s3 = boto3.resource('s3')

    parsed = urlparse(s3_path)
    bucket = parsed.netloc
    key = parsed.path.lstrip('/')

    obj = s3.Object(bucket, key)

    # https://stackoverflow.com/a/3305964/4355916
    return obj.get()['Body'].read().decode('utf-8')


def bulk_fires_to_tile(s3_path):

    s3_obj = s3_to_object(s3_path)
    f = StringIO(s3_obj)

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


def update_geopackage(src_gpkg, fire_list, fire_type):

    temp_gpkg = '/tmp/temp.gpkg'

    feature_template = {'geometry': {'type': 'Point', 'coordinates': ()},
                                     'type': 'Feature', 'properties': OrderedDict()}

    # if we have a lot of fires to update, fire_list may be a string pointing
    # to an s3 object (s3://palm-risk-poc/ . . . 
    if isinstance(fire_list, basestring):
        fire_list = read_fire_list_from_s3(fire_list)

    # get list of dates we're updating
    # don't want any overlap between dates we're updating an data in gpkg
    update_dates = set([datetime.datetime.strptime(x['fire_date'], '%m/%d/%Y %H:%M:%S').date() for x in fire_list])

    # also want to remove any fires > 1 year old, because they're out of the scope of this project
    one_year_ago = datetime.datetime.now().date() - relativedelta(years=1)

    # open source geopackage to get older fires data
    with fiona.open(src_gpkg) as src:

        # open new temporary output for writing
        with fiona.open(temp_gpkg, 'w', schema=schema, driver='GPKG', layer='data') as dst:

            # copy existing fires to new database
            for record in src:
                record_date = datetime.datetime.strptime(record['properties']['fire_date'], '%Y-%m-%d').date()
                record_type = record['properties']['fire_type']

                # make sure we're only saving "new" data
                if record_date >= one_year_ago:

                    # if we're updating this date + fire type, don't write old data
                    if record_date in update_dates and record_type == fire_type:
                        pass
                    else:
                        dst.write(record)

            # write new files based on the input data
            for new_fire in fire_list:
                new_feature = feature_template.copy()
                new_feature['geometry']['coordinates'] = (float(new_fire['lon']), float(new_fire['lat']))
                new_feature['properties']['fire_type'] = new_fire['fire_type']

                # read in timtestamp as string, convert to date, then format to match GPKG standard
                formatted_date = datetime.datetime.strptime(new_fire['fire_date'], '%m/%d/%Y %H:%M:%S').date().strftime('%Y-%m-%d')
                new_feature['properties']['fire_date'] = formatted_date
                dst.write(new_feature)

    return temp_gpkg


def write_fire_tile_to_s3(fire_list, fire_type, tile_id):
    
    date_fmt = datetime.datetime.today().date().strftime('%Y%m%d')
    out_s3_path = 'temp/fires/{}/{}/{}.json'.format(fire_type, date_fmt, tile_id)

    s3 = boto3.client('s3')
    bucket_name = 'palm-risk-poc'
    s3.put_object(Bucket=bucket_name, Key=out_s3_path, Body=json.dumps(fire_list))

    return 's3://{}/{}'.format(bucket_name, out_s3_path)


def read_fire_list_from_s3(s3_path):

    s3_obj = s3_to_object(s3_path)
    return json.loads(s3_obj)

