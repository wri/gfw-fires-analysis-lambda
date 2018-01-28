from unittest import TestCase
import os
import sys
import json
import logging

root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
lambda_dir = os.path.join(root_dir, "lambda")

sys.path.append(lambda_dir)

import lambda_handler


class TestLambda(TestCase):

    payload = {
        "filename": "s3://gfw2-data/alerts-tsv/temp/10N_020E_biomass.tif",
        "bbox": [20.999, 5, 21, 4.999],
        "s3_output": "s3://palm-risk-poc/raster-to-point/test.tsv"
    }

    def test_load_lambda(self):
        """ Test the lambda handler is loaded """
        self.assertTrue(hasattr(lambda_handler, "handler"))

    def test_run_lambda(self):
        """ Run the lambda handler with test payload """
        result = lambda_handler.handler(self.payload, None)
        msg = json.loads(result['body'])['msg']
        correct_msg = 'fiona imported!'
        self.assertEqual(correct_msg, msg)

