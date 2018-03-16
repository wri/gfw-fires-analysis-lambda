import boto3
import json


client = boto3.client('lambda')

# first, upload a day's worth of fires (one type only) to
# somewhere on s3
fire_type = 'VIIRS'
fire_csv = 's3://gfw2-data/alerts-tsv/temp/fires/update20180312/20180307_viirs.csv'

# then build an event to kick off the process
event = {'queryStringParameters': {'fire_csv': fire_csv, 'fire_type': fire_type}}

# invoke our bulk upload function, which will download the
# fire_csv above, split it into 1x1 tiles, then kick off
# per-tile lambda functions to store this data
client.invoke(
        FunctionName='fire-alerts-dev-bulk-fire-upload',
        InvocationType='Event',
        Payload=json.dumps(event))
