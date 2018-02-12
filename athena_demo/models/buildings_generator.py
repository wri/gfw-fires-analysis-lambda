from io import StringIO

import boto3
import pandas as pd
from shapely.geometry import Polygon

from models.utils import AthenaWaiter, download_file_from_s3


class BuildingsGenerator(object):

    def __init__(self, geom_wkt, min_date):
        self.geom_wkt = geom_wkt
        self.min_date = min_date

        self.bucket = 'gfw2-data'
        self.folder = 'alerts-tsv/temp/fires-analysis'

    def get_query_string(self):

        return ("SELECT fire_type, fire_date, count(*) as fire_count "
       "FROM FIRES "
       "WHERE fire_date > Date('{}') AND "
       "ST_Intersects(ST_Polygon('{}'), ST_Point(lon, lat)) "
       "GROUP BY fire_type, fire_date " ).format(self.min_date, self.geom_wkt)

    def get_query_id(self):
        client = boto3.client(
            'athena',
            region_name='us-east-1'
        )
        response = client.start_query_execution(
            QueryString=self.get_query_string(),
            QueryExecutionContext={
                'Database': 'default'
            },
            ResultConfiguration={
                'OutputLocation': 's3://{0}/{1}'.format(
                    self.bucket,
                    self.folder
                )
            }
        )
        return response['QueryExecutionId']

    def get_results_key(self, query_id):
        return '{0}/{1}.csv'.format(self.folder, query_id)

    def get_results_df(self, query_id):
        waiter = AthenaWaiter(max_tries=100)
        waiter.wait(
            bucket=self.bucket,
            key=self.get_results_key(query_id),
            query_id=query_id
        )
        raw_result = StringIO(
            download_file_from_s3(
                self.get_results_key(query_id),
                self.bucket
            )
        )
        return pd.read_csv(raw_result, encoding='utf-8')

    def generate(self):
        query_id = self.get_query_id()
        print self.get_query_string()
        return self.get_results_df(query_id)
