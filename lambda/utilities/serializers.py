import json


def http_response(response):

    # print json.dumps(response, indent=4, sort_keys=True)

    return {
        'statusCode': 200,
        'headers': {'Access-Control-Allow-Origin': '*'},
        'body': json.dumps(response)
            }


def serialize_fire_analysis(date_list, tile_id):
    print date_list
    return http_response({tile_id: date_list})


def api_error(msg):
    print msg

    return {
        'statusCode': 400,
        'headers': {'Access-Control-Allow-Origin': '*'},
        'body': json.dumps({'error': msg})
            }
