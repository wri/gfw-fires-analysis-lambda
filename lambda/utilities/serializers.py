import json


def http_response(response):

    return {
        'statusCode': 200,
        'headers': {'Access-Control-Allow-Origin': '*'},
        'body': json.dumps(response)
            }


def serialize_fire_alerts(date_list, params):

    agg_by = params['aggregate_by']

    if agg_by != 'all':
        date_list = date_list[agg_by]

    serialized = {
    "data": {
        "aggregate_by": agg_by,
        "aggregate_values": True,
        "attributes": {
            "downloadUrls": None,
            "value": date_list},
        "period": params['period'],
        "type": "fire-alerts",
        "fire-type": "VIIRS"
        }
    }

    return http_response(serialized)


def api_error(msg):
    print msg
    return {
        'statusCode': 400,
        'headers': {'Access-Control-Allow-Origin': '*'},
        'body': json.dumps({'error': msg})
            }


def serialize_layer_extent(layer, geom_is_valid):
   
    serialized = {
    "data": {
        "attributes": {"geom-within-coverage": geom_is_valid},
        "type": layer
        }
    }

    return http_response(serialized)