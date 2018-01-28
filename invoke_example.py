import boto3
import json


client = boto3.client('lambda')

# example landsat base path
landsat_base = 's3://landsat-pds/L8/001/002/LC80010022016230LGN00'

# for some reason each band has it's own TIF
# or so it would seem from the filenames
# iterate over bands 1 - 11 to demonstrate lambda calcs
for band_id in range(1, 12):

    filename = '{}/LC80010022016230LGN00_B{}.TIF'.format(landsat_base, band_id)

    # parameters for the lambda function
    # in this case just a filename to calculate stats on
    event = {'filename': filename}

    client.invoke(
        FunctionName='raster-to-point-dev-receiver',
        InvocationType='Event',
        Payload=json.dumps(event))
