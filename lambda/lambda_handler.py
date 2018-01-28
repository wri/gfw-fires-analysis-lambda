import os
import sys
import logging

# add path to included packages
path = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.join(path, 'lib/python2.7/site-packages'))

import fiona
import raster_to_point
import serializers


# set up logger
logger = logging.getLogger(__file__)
logger.setLevel(logging.DEBUG)
# commented out to avoid duplicate logs in lambda
# logger.addHandler(logging.StreamHandler())


def handler(event, context):

    """ Lambda handler """
    logger.debug(event)

    return serializers.http_response({'msg': 'fiona imported!'}) 
