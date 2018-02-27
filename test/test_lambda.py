from unittest import TestCase
import os
import sys
import json
import logging
import datetime
from dateutil.relativedelta import relativedelta
import copy


root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
lambda_dir = os.path.join(root_dir, "lambda")

sys.path.append(lambda_dir)

import handler

class TestAlerts(TestCase):

    def setUp(self):
        aoi = {"type":"FeatureCollection","features":[{"type":"Feature","properties":{},"geometry":{"type":"Polygon","coordinates":[[[117.164337416055,-0.146001213786356],[117.155703106841,-0.057749439573639],[117.130133243555,0.027110960960791],[117.088610223703,0.105318959360494],[117.032729390131,0.173868987816331],[116.9646379261,0.230126442445014],[116.886952514797,0.271929022470392],[116.802658856842,0.297669979066676],[116.714996872182,0.306360018259741],[116.62733601195,0.297665417784521],[116.543045531146,0.271920848997643],[116.465364792161,0.230116400643847],[116.397278664419,0.173859331577133],[116.341402857481,0.105312081565839],[116.299883592981,0.027108993740147],[116.274315418049,-0.057744991082702],[116.265680230058,-0.145989746845834],[116.274309760144,-0.234235059943345],[116.299872885489,-0.319090657019565],[116.341388234208,-0.397296240890491],[116.397261631207,-0.465846611406709],[116.465347025389,-0.522107112718315],[116.543028657773,-0.563914980303813],[116.627321403215,-0.589662660364852],[116.714985480799,-0.598359835271131],[116.80265112075,-0.589671696300947],[116.886948340271,-0.56393193102085],[116.964636750894,-0.522129896874692],[117.032730315149,-0.465872485244865],[117.088612191345,-0.397322188160817],[117.130135233987,-0.319113812726435],[117.155704320927,-0.234253104759971],[117.164337416055,-0.146001213786356]]]}}]}

        aggregate_by = 'week'

        fire_type = 'modis'
        payload = {
            'body': json.dumps({'geojson': aoi}),
            'queryStringParameters': {'aggregate_values': 'true', 'aggregate_by': aggregate_by, 'fire_type': fire_type}
        }

        result = handler.fire_alerts(payload, None)

        self.date_list = json.loads(result['body'])['data']['attributes']['value']


    def test_run_alerts(self):

        test_count = [x for x in self.date_list if x['week'] == 1][0]['count']

        self.assertEqual(test_count, 37)

class TestBogusInputs(TestCase):

    def setUp(self):
        aoi = {"type":"FeatureCollection","features":[{"type":"Feature","properties":{},"geometry":{"type":"Polygon","coordinates":[[[117.164337416055,-0.146001213786356],[117.155703106841,-0.057749439573639],[117.130133243555,0.027110960960791],[117.088610223703,0.105318959360494],[117.032729390131,0.173868987816331],[116.9646379261,0.230126442445014],[116.886952514797,0.271929022470392],[116.802658856842,0.297669979066676],[116.714996872182,0.306360018259741],[116.62733601195,0.297665417784521],[116.543045531146,0.271920848997643],[116.465364792161,0.230116400643847],[116.397278664419,0.173859331577133],[116.341402857481,0.105312081565839],[116.299883592981,0.027108993740147],[116.274315418049,-0.057744991082702],[116.265680230058,-0.145989746845834],[116.274309760144,-0.234235059943345],[116.299872885489,-0.319090657019565],[116.341388234208,-0.397296240890491],[116.397261631207,-0.465846611406709],[116.465347025389,-0.522107112718315],[116.543028657773,-0.563914980303813],[116.627321403215,-0.589662660364852],[116.714985480799,-0.598359835271131],[116.80265112075,-0.589671696300947],[116.886948340271,-0.56393193102085],[116.964636750894,-0.522129896874692],[117.032730315149,-0.465872485244865],[117.088612191345,-0.397322188160817],[117.130135233987,-0.319113812726435],[117.155704320927,-0.234253104759971],[117.164337416055,-0.146001213786356]]]}}]}

        aggregate_by = 'week'

        fire_type = 'modis'
        self.payload = {
            'body': json.dumps({'geojson': aoi}),
            'queryStringParameters': {'aggregate_values': 'true', 'aggregate_by': aggregate_by, 'fire_type': fire_type}
                    }

    def run_fire_alerts(self, payload):
        result = handler.fire_alerts(payload, None)
        result_body = json.loads(result['body'])

        result_message = result_body['error']

        return result_message

    def test_start_date_after_end_date(self):
        payload = copy.deepcopy(self.payload)

        today = datetime.datetime.now().date()
        last_month = today - relativedelta(months=1)

        # should be last_month date, then today
        period = today.strftime('%Y-%m-%d') + ',' + last_month.strftime('%Y-%m-%d')
        payload['queryStringParameters']['period'] = period

        self.assertEqual(self.run_fire_alerts(payload), 'Start date must be <= end date')

    def test_bogus_period(self):
        payload = copy.deepcopy(self.payload)

        today = datetime.datetime.now().date()
        last_month = today - relativedelta(months=1)

        # proper strftime format is '%Y-%m-%d'
        period = last_month.strftime('%y-%m-%d') + ',' + today.strftime('%y-%m-%d')
        payload['queryStringParameters']['period'] = period

        self.assertEqual(self.run_fire_alerts(payload), 'period must be formatted as YYYY-mm-dd,YYYY-mm-dd')

    def test_start_date_greater_than_one_year(self):
        payload = copy.deepcopy(self.payload)

        today = datetime.datetime.now().date()
        last_year_and_one_day = today - relativedelta(years=1, days=1)

        period = last_year_and_one_day.strftime('%Y-%m-%d') + ',' + today.strftime('%Y-%m-%d')
        payload['queryStringParameters']['period'] = period

        self.assertEqual(self.run_fire_alerts(payload), 'Start date must be more recent than one year ago')

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

        self.assertEqual(self.run_fire_alerts(payload), 'You must supply an aggregate_by param: year, quarter, month, week, day, all')

    def test_agg_values_false(self):
        payload = copy.deepcopy(self.payload)
        payload['queryStringParameters']['aggregate_values'] = False

        self.assertEqual(self.run_fire_alerts(payload), 'aggregate_values must be set to true')

    def test_bad_firetype(self):
        payload = copy.deepcopy(self.payload)
        payload['queryStringParameters']['fire_type'] = 'merdis'

        valid_fire_list = ['viirs', 'modis', 'all']

        self.assertEqual(self.run_fire_alerts(payload), 'For this batch service, fire_type must be in {}'.format(', '.join(valid_fire_list)))

    def test_garbage_geojson(self):
        aoi = {"type":"FeatureCollection","features":[{"type":"Feature","properties":{},"geometry":{"type":"Polygon","coordinates":[]}}]}

        payload = copy.deepcopy(self.payload)
        payload['body'] = json.dumps({'geojson': aoi})

        self.assertEqual(self.run_fire_alerts(payload), 'Unable to decode input geojson')

class TestAnalysis(TestCase):

    def setUp(self):
        aoi = {"type":"FeatureCollection","features":[{"type":"Feature","properties":{},"geometry":{"type":"Polygon","coordinates":[[[117.164337416055,-0.146001213786356],[117.155703106841,-0.057749439573639],[117.130133243555,0.027110960960791],[117.088610223703,0.105318959360494],[117.032729390131,0.173868987816331],[116.9646379261,0.230126442445014],[116.886952514797,0.271929022470392],[116.802658856842,0.297669979066676],[116.714996872182,0.306360018259741],[116.62733601195,0.297665417784521],[116.543045531146,0.271920848997643],[116.465364792161,0.230116400643847],[116.397278664419,0.173859331577133],[116.341402857481,0.105312081565839],[116.299883592981,0.027108993740147],[116.274315418049,-0.057744991082702],[116.265680230058,-0.145989746845834],[116.274309760144,-0.234235059943345],[116.299872885489,-0.319090657019565],[116.341388234208,-0.397296240890491],[116.397261631207,-0.465846611406709],[116.465347025389,-0.522107112718315],[116.543028657773,-0.563914980303813],[116.627321403215,-0.589662660364852],[116.714985480799,-0.598359835271131],[116.80265112075,-0.589671696300947],[116.886948340271,-0.56393193102085],[116.964636750894,-0.522129896874692],[117.032730315149,-0.465872485244865],[117.088612191345,-0.397322188160817],[117.130135233987,-0.319113812726435],[117.155704320927,-0.234253104759971],[117.164337416055,-0.146001213786356]]]}}]}
        tile_id = '00N_116E'
        payload = {
            'body': json.dumps({'geojson': aoi}),
            'queryStringParameters': {'tile_id': tile_id, 'fire_type': 'all', 'period': '2016-01-01,2017-01-01'}
        }

        result = handler.fire_analysis(payload, None)

        self.date_list = json.loads(result['body'])[tile_id]


    def test_run_lambda_len(self):
        """ Run the lambda handler with test payload """

        self.assertEqual(len(self.date_list), 135)


    def test_run_lambda_count(self):

        test_date = '2016-01-09'
        test_count = self.date_list[test_date]
        self.assertEqual(test_count, 65)
