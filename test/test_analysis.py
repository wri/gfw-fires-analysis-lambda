from unittest import TestCase
import os
import sys
import json
import logging
import datetime
from dateutil.relativedelta import relativedelta
import copy
from shapely.geometry import shape

root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
lambda_dir = os.path.join(root_dir, "lambda")

sys.path.append(lambda_dir)

from utilities import geoprocessing


class TestAnalysis(TestCase):

    def setUp(self):
        aoi = {"type": "FeatureCollection", "name": "test_geom",
               "crs": {"type": "name", "properties": {"name": "urn:ogc:def:crs:OGC:1.3:CRS84"}}, "features": [
                {"type": "Feature", "properties": {"id": 1}, "geometry": {"type": "Polygon", "coordinates": [
                    [[19.607011190476182, -8.765827380952395], [25.652106428571422, -14.702001984126998],
                     [25.652106428571422, -14.702001984126998], [21.485892142857136, -19.848501984126997],
                     [14.406050873015866, -17.397787698412714], [14.215439761904756, -11.162081349206364],
                     [19.607011190476182, -8.765827380952395]]]}}]}

        payload = {
            'body': json.dumps({'geojson': aoi}),
            'queryStringParameters': {'period': '2016-01-01,2017-01-01'}
        }
        geom = shape(aoi['features'][0]['geometry'])
        period = '2017-01-01,2017-12-01'
        local_geopackage = '/home/geolambda/test/data.gpkg'
        result = geoprocessing.point_stats(geom, period, local_geopackage)

        print "\n******RESULT: {}".format(result)
        self.date_list = json.loads(result)

    def test_run_lambda_len(self):
        """ Run the lambda handler with test payload """

        self.assertEqual(len(self.date_list), 135)

    def test_run_lambda_count(self):

        test_date = '2016-01-09'
        test_count = self.date_list[test_date]
        self.assertEqual(test_count, 65)
