import csv
import datetime
import io
from urlparse import urlparse

import boto3

s3 = boto3.resource('s3')


def get_s3_file_list(s3_dir):

    bucket, prefix = get_bucket_and_prefix(s3_dir)

    # loop through file names in the bucket
    # https://stackoverflow.com/a/32636026/4355916
    client = boto3.client('s3')
    full_path_list = [x['Key'] for x in client.list_objects(Bucket=bucket.name, Prefix=prefix)['Contents']]

    # unpack the filename from the list of files
    filename_only_list = [x.split('/')[-1] for x in full_path_list]

    return filename_only_list


def filter_s3_dir_by_date(s3_dir, fire_type, update_date):

    file_list = get_s3_file_list(s3_dir)
    file_list = [x for x in file_list if update_date in x]

    if len(file_list) != 6:
        # log to lambda console
        print 'Found {} files for {}, expected 6'.format(fire_type, len(file_list))

    return file_list


def get_bucket_and_prefix(s3_dir):

    parsed = urlparse(s3_dir)

    # connect to the s3 bucket
    bucket = s3.Bucket(parsed.netloc)

    # remove leading slash, for some reason
    prefix = parsed.path[1:]

    return bucket, prefix


def combine(fire_type, fire_date):

    s3_dir = 's3://gfw2-data/fires/fires_for_elasticsearch/{}/'.format(fire_type)

    csv_list = filter_s3_dir_by_date(s3_dir, fire_type, fire_date)

    bucket, prefix = get_bucket_and_prefix(s3_dir)

    # create out CSV text string
    out_csv = io.BytesIO()
    writer = csv.writer(out_csv)

    # write header row
    writer.writerow(['LATITUDE', 'LONGITUDE', 'ACQ_DATE', 'FIRE_TYPE'])

    # iterate over the file list, reading each in as string
    for s3_csv in csv_list:
        key = '{}{}'.format(prefix, s3_csv)

        # https://stackoverflow.com/a/35376156/4355916
        print key
        obj = s3.Object(bucket.name, key)
        lines = obj.get()['Body'].read().splitlines()

        reader = csv.DictReader(lines)
        for row in reader:

            # format row date into m/d/Y expected later on
            row_date = datetime.datetime.strptime(row['fire_datetime'], '%Y/%m/%d %H:%M:%S')
            row_date_fmt = row_date.strftime('%Y/%m/%d')

            writer.writerow([row['latitude'], row['longitude'], row_date_fmt, fire_type])

    output_bucket = 'palm-risk-poc'
    output_key = 'data/fires-export/{}-{}.csv'.format(fire_type, fire_date)
       
    s3_output = s3.Object(output_bucket, output_key)
    s3_output.put(Body=out_csv.getvalue())

    return 's3://{}/{}'.format(output_bucket, output_key)

