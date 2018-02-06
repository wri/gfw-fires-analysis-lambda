import json


def http_response(response):

    # print json.dumps(response, indent=4, sort_keys=True)

    return {
        'statusCode': 200,
        'headers': {'Access-Control-Allow-Origin': '*'},
        'body': json.dumps(response)
            }


def serialize_fire_analysis(date_list, tile_id):

    return http_response({tile_id: date_list})


def serialize_fire_alerts(date_list, agg_by):

    if agg_by != 'all':
        date_list = date_list[agg_by]

    serialized = {
    "data": {
        "aggregate_by": agg_by,
        "aggregate_values": True,
        "attributes": {
            "downloadUrls": None,
            "value": date_list},
        "period": None,
        "type": "fire-alerts"}
    }

    return http_response(serialized)

def api_error(msg):
    print msg
    return {
        'statusCode': 400,
        'headers': {'Access-Control-Allow-Origin': '*'},
        'body': json.dumps({'error': msg})
            }
