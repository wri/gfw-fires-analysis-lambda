from unittest import TestCase
import os
import sys
import json
import logging

root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
lambda_dir = os.path.join(root_dir, "lambda")

sys.path.append(lambda_dir)

import handler


class TestLambda(TestCase):
    aoi = {"type":"FeatureCollection","features":[{"type":"Feature","properties":{},"geometry":{"type":"Polygon","coordinates":[[[117.164337416055,-0.146001213786356],[117.155703106841,-0.057749439573639],[117.130133243555,0.027110960960791],[117.088610223703,0.105318959360494],[117.032729390131,0.173868987816331],[116.9646379261,0.230126442445014],[116.886952514797,0.271929022470392],[116.802658856842,0.297669979066676],[116.714996872182,0.306360018259741],[116.62733601195,0.297665417784521],[116.543045531146,0.271920848997643],[116.465364792161,0.230116400643847],[116.397278664419,0.173859331577133],[116.341402857481,0.105312081565839],[116.299883592981,0.027108993740147],[116.274315418049,-0.057744991082702],[116.265680230058,-0.145989746845834],[116.274309760144,-0.234235059943345],[116.299872885489,-0.319090657019565],[116.341388234208,-0.397296240890491],[116.397261631207,-0.465846611406709],[116.465347025389,-0.522107112718315],[116.543028657773,-0.563914980303813],[116.627321403215,-0.589662660364852],[116.714985480799,-0.598359835271131],[116.80265112075,-0.589671696300947],[116.886948340271,-0.56393193102085],[116.964636750894,-0.522129896874692],[117.032730315149,-0.465872485244865],[117.088612191345,-0.397322188160817],[117.130135233987,-0.319113812726435],[117.155704320927,-0.234253104759971],[117.164337416055,-0.146001213786356]]]}}]}
    tile_id = '10N_00W'
    payload = {
        'body': json.dumps({'geojson': aoi}),
        'queryStringParameters': {'tile_id': tile_id}

    }

    def test_load_lambda(self):
        """ Test the lambda handler is loaded """
        self.assertTrue(hasattr(handler, "fire_alerts"))

    def test_run_lambda(self):
        """ Run the lambda handler with test payload """
        result = handler.fire_analysis(self.payload, None)
        print result
        date_list = json.loads(result['body'])[self.tile_id]

        self.assertEqual(len(date_list), 167)
