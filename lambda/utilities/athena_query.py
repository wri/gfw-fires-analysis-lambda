import datetime
from io import StringIO
from dateutil.relativedelta import relativedelta

import boto3
from shapely.geometry import Polygon

from athena_utils import AthenaWaiter, get_s3_results_in_dict


class FirePointsGenerator(object):

    def __init__(self, geom_wkt, tile_id, fire_type_list, period):
        self.geom_wkt = geom_wkt
        self.tile_id = tile_id
        self.fire_type_list = fire_type_list
        self.period = period

        self.bucket = 'palm-risk-poc'
        self.folder = 'output/fires'

    def get_query_string(self):
        # only get fire alerts for the past 1 year. ex period: '2016-01-01,2017-01-01'
        if self.period:
            min_date = self.period.split(',')[0]
            max_date = self.period.split(',')[-1]
        else:
            today = datetime.datetime.now()
            min_date = (today - relativedelta(years=2)).strftime('%Y-%m-%d')
            max_date = today.strftime('%Y-%m-%d')
        # go from [x, y, z] to (x, y, z)
        sql_fire_type_list = "('{}')".format("', '".join(self.fire_type_list))

        return ('''SELECT fire_date, count(*) as fire_count '''
       '''FROM "{tile_id}" '''
       '''WHERE Date('{min_date}') <= fire_date and fire_date <= Date('{max_date}') AND '''
       '''fire_type in {fire_type_list} AND '''
       '''ST_Intersects(ST_Polygon('{geom_wkt}'), ST_Point(lon, lat)) '''
       '''GROUP BY fire_date ''' ).format(tile_id=self.tile_id.lower(),
                                            min_date=min_date,
                                            max_date=max_date,
                                            geom_wkt=self.geom_wkt,
                                            fire_type_list=sql_fire_type_list)

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
        date_count_dict = get_s3_results_in_dict(
                self.get_results_key(query_id),
                self.bucket
        )

        return date_count_dict


    def generate(self):
        query_id = self.get_query_id()
        return self.get_results_df(query_id)
