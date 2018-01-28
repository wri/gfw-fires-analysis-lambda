import json


def http_response(response):

    print json.dumps(response, indent=4, sort_keys=True)

    return {
        'statusCode': 200,
        'headers': {'Access-Control-Allow-Origin': '*'},
        'body': json.dumps(response)
            }
