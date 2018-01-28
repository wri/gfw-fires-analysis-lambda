## Raster Processing on Serverless Architecture

Code to parallelize raster-to-point operations and write the output to S3

This is currently in development, but will ultimately be used to convert global Hansen or biomass data to a format usable for our hadoop point in polygon process.

Proposed inputs:
- S3 VRT of geotifs to vectorize
- bbox of area to vectorize
- output_path of s3 file to write

### Development

1. install dependencies - docker, serverless CLI
2. Test existing code
```
docker-compose run test
```
3. Make changes, then publish
```
docker compose run package
serverless deploy -v
```
4. Test invocation with python
```
python invoke_example.py
```
5. Check output. Currently writing files here:
```
s3://palm-risk-poc/raster-to-point/
```

### Processing raster data

This is largely TBD until we get a process going but hopefully we'll have something like this:

`python tif-to-points.py -t <vrt path> -g <grid shp> -i <id fieldname>`

### Acknowledgements

Thanks to Matt Hanson and the Development Seed team for building such a solid geolambda infrastructure and testing environment: https://github.com/developmentseed/geolambda/

And to Matthew Mcfarland and Azavea for introducing me to the serverless CLI with his rasterio lambda project: https://github.com/mmcfarland/foss4g-lambda-demo
