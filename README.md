# gfw-fires-analysis-lambda

### Background
This repo uses AWS lambda and AWS Athena to batch process fire counts.

This usage grew out of the need to process 2000 palm oil mills in under 5 minutes, per the requirements for the upcoming GFW Pro application.

### Endpoints

The endpoints deployed are designed to exactly mimic existing GFW API endpoints, making it easy to 'plug' this service into existing code.

Base URL:
https://u81la7we82.execute-api.us-east-1.amazonaws.com/dev/

/fire-alerts
Generates a list of 10x10 degree tiles that the geometry intersects. Sends the tile_ids and input params to fire-analysis endpoint

/fire-analysis
Runs analysis for the 10x10 degree tile that geometry intersects. Returns a nested dictionary of tile id and counts of points per fire date

## Limitations
Athena costs $5 per TB of data queried. To reduce amount of data queried, we will split each request into tiles (parallel query).

## Development
1. Clone locally

2. Create .env file to store AWS credentials:
```
AWS_ACCESS_KEY_ID=<my key id>
AWS_SECRET_ACCESS_KEY=<my key>
```

3. Run lambda/handler.py to test analysis and alerts endpoints (see `if __name__ == '__main__':` block)

4. Build docker container `docker-compose build`

5. Run `docker-compose run test`. This will execute the tests in test/test_lambda.py in the right environment

## Deployment
1. install serverless `npm install -g serverless`

2. Package - `docker-compose run package`

3. Deploy - `serverless deploy -v`
