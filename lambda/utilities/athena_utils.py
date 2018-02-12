import time

import boto3
import botocore
from botocore.client import Config
from io import StringIO
import csv


def get_s3_client():
    return boto3.client(
        's3', 'us-east-1', config=Config(
            s3={'addressing_style': 'path'}
        )
    )


def get_s3_results_in_dict(key, bucket):
    s3_client = get_s3_client()
    data = s3_client.get_object(
        Bucket=bucket,
        Key=key
    )

    s3_result_uni = data['Body'].read().decode("utf-8")
    stringio_s3 = StringIO(s3_result_uni)

    # reader is returned as line seperated lists: ['2016-05-07', '208']
    reader = csv.reader(stringio_s3, skipinitialspace=True)

    # skip header row
    next(reader, None)

    date_count_dict = {}
    for row in reader:
        date_count_dict[row[0]] = int(row[1])

    return date_count_dict


class AthenaWaiterException(Exception):
    pass


class AthenaWaiter(object):
    """Not only can wait more than the AWS S3 waiter,
    but it also checks if the query has failed
    or was canceled and stops instead of waiting
    until it times out.
    """

    def __init__(self, max_tries=30, interval=1):
        self.s3_client = get_s3_client()
        self.athena_client = boto3.client(
            'athena',
            region_name='us-east-1'
        )
        self.max_tries = max_tries
        self.interval = interval

    def object_exists(self, bucket='', key=''):
        exists = True
        try:
            self.s3_client.head_object(Bucket=bucket, Key=key)
        except botocore.exceptions.ClientError as exc:
            if exc.response['Error']['Code'] == '404':
                exists = False
            else:
                raise
        return exists

    def check_status(self, query_id):
        status = self.athena_client.get_query_execution(
            QueryExecutionId=query_id
        )['QueryExecution']['Status']
        if status['State'] in ['FAILED', 'CANCELLED']:
            raise AthenaWaiterException(
                'Query Error: {0}'
                .format(status['StateChangeReason'])
            )

    def wait(self, bucket='', key='', query_id=''):

        success = False
        for _ in range(self.max_tries):
            if self.object_exists(bucket=bucket, key=key):
                success = True
                break
            self.check_status(query_id)
            time.sleep(self.interval)
        if not success:
            raise AthenaWaiterException(
                'Exceeded the maximum number of tries ({0})'
                .format(self.max_tries)
            )
