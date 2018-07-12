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

import handler


class TestBogusInputs(TestCase):

    def setUp(self):

        aoi = {"type": "FeatureCollection", "name": "test_geom",
               "crs": {"type": "name", "properties": {"name": "urn:ogc:def:crs:OGC:1.3:CRS84"}}, "features": [
                {"type": "Feature", "properties": {"id": 1}, "geometry": {"type": "Polygon", "coordinates": [
                    [[19.607011190476182, -8.765827380952395], [25.652106428571422, -14.702001984126998],
                     [25.652106428571422, -14.702001984126998], [21.485892142857136, -19.848501984126997],
                     [14.406050873015866, -17.397787698412714], [14.215439761904756, -11.162081349206364],
                     [19.607011190476182, -8.765827380952395]]]}}]}

        aggregate_by = 'week'

        self.payload = {
            'body': json.dumps({'geojson': aoi}),
            'queryStringParameters': {'aggregate_values': 'true', 'aggregate_by': aggregate_by}
                    }

    def run_fire_alerts(self, payload):
        result = handler.fire_alerts(payload, None)
        result_body = json.loads(result['body'])

        result_message = result_body['error']

        return result_message

    def test_bad_geom(self):
        aoi = {"type":"FeatureCollection","features":[{"type":"Feature","properties":{},"geometry":{"type":"Point","coordinates":[114.2578125,2.4601811810210052]}}]}

        payload = copy.deepcopy(self.payload)
        payload['body'] = json.dumps({'geojson': aoi})

        self.assertEqual(self.run_fire_alerts(payload), 'Geometry type must be polygon or multipolygon')

    def test_no_geom(self):
        payload = copy.deepcopy(self.payload)
        payload['body'] = json.dumps({'geojson': ''})

        self.assertEqual(self.run_fire_alerts(payload), 'Unable to decode input geojson')

    def test_no_params(self):
        payload = copy.deepcopy(self.payload)
        payload['queryStringParameters'] = {}

        self.assertEqual(self.run_fire_alerts(payload), 'aggregate_values must be set to true')

    def test_no_aggregate_values_para(self):
        payload = copy.deepcopy(self.payload)
        del payload['queryStringParameters']['aggregate_values']

        self.assertEqual(self.run_fire_alerts(payload), 'aggregate_values must be set to true')

    def test_bad_agg_by(self):
        payload = copy.deepcopy(self.payload)
        payload['queryStringParameters']['aggregate_by'] = 'werk'

        self.assertEqual(self.run_fire_alerts(payload), 'You must supply an aggregate_by param: day')

    def test_agg_values_false(self):
        payload = copy.deepcopy(self.payload)
        payload['queryStringParameters']['aggregate_values'] = False

        self.assertEqual(self.run_fire_alerts(payload), 'aggregate_values must be set to true')

    def test_garbage_geojson(self):
        aoi = {"type":"FeatureCollection","features":[{"type":"Feature","properties":{},"geometry":{"type":"Polygon","coordinates":[]}}]}

        payload = copy.deepcopy(self.payload)
        payload['body'] = json.dumps({'geojson': aoi})

        self.assertEqual(self.run_fire_alerts(payload), 'Unable to decode input geojson')
