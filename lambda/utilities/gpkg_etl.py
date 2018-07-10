import subprocess
import csv
import sqlite3
import os

from datetime import timedelta, datetime
from urlparse import urlparse
from StringIO import StringIO
import boto3

import util


def s3_to_csv_reader(s3_path):

    s3 = boto3.resource('s3')

    parsed = urlparse(s3_path)
    bucket = parsed.netloc
    key = parsed.path.lstrip('/')

    obj = s3.Object(bucket, key)

    # https://stackoverflow.com/a/3305964/4355916
    as_str = StringIO(obj.get()['Body'].read().decode('utf-8').encode('utf-8'))

    return as_str.getvalue().splitlines()


def get_bucket():

    s3 = boto3.resource('s3')
    bucket = s3.Bucket('palm-risk-poc')

    return bucket


def download_gpkg():

    # weirdly this seems 2x - 3x as fast as reading directly from s3
    # maybe spatial index isn't used when reading from s3?
    bucket = get_bucket()

    gpkg_src = 'data/fires-one-gpkg/data.gpkg'
    local_gpkg = '/tmp/data.gpkg'

    bucket.download_file(gpkg_src, local_gpkg)

    return local_gpkg


def delete_dups_and_old_fires(src_gpkg):
    # get the date of 10 days ago
    date_10_days_ago = datetime.now() - timedelta(days=10)

    # connect to GPKG and delete any duplicate data
    conn = sqlite3.connect(src_gpkg)
    cur = conn.cursor()

    sql_str = ("DELETE FROM data "
               "WHERE rowid NOT IN ( "
               "SELECT min(rowid) "
               "FROM data "
               "GROUP BY geom, fire_date);")
    cur.execute(sql_str)

    delete_old_fires_sql = ('DELETE FROM data '
                            'WHERE fire_date < "{}"'.format(date_10_days_ago))

    cur.execute(delete_old_fires_sql)
    conn.commit()
    conn.close()


def upload_gpkg(src_gpkg):
    bucket = get_bucket()
    gpkg_dst = 'data/fires-one-gpkg/data.gpkg'

    bucket.upload_file(src_gpkg, gpkg_dst)


def update_geopackage(s3_path):

    # Read fires csv directly from s3
    fires = [x for x in csv.DictReader(s3_to_csv_reader(s3_path))]

    # open source geopackage to write new data
    src_gpkg = download_gpkg()

    # read in csv and write it back with correct format
    fires_formatted = util.fix_csv_date_lines(fires)

    # make vrt of source fires
    fires_vrt = util.create_vrt(fires_formatted)

    # append new fires to gpkg
    path = os.path.dirname(os.path.realpath(__file__))
    path = os.path.dirname(path)
    ogr2ogr = os.path.join(path, 'lib/ogr2ogr')

    cmd = [ogr2ogr, '-append', src_gpkg, fires_vrt, '-f', 'GPKG', '-nln', 'data']
    subprocess.check_call(cmd)

    # remove old and duplicate points
    delete_dups_and_old_fires(src_gpkg)

    # upload gpkg back to s3
    upload_gpkg(src_gpkg)

    return None