import datetime

from shapely.geometry import shape

from models.buildings_generator import BuildingsGenerator


geojson = {"type":"FeatureCollection","features":[{"type":"Feature","properties":{},"geometry":{"type":"Polygon","coordinates":[[[18.720703125,-1.4720060101903352],[21.9287109375,-6.489983332670651],[26.82861328125,-3.7107820043487076],[26.47705078125,-0.9447814006873896],[22.8076171875,-1.3841426927920029],[21.796875,-0.10986321392741416],[18.720703125,-1.4720060101903352]]]}}]}

geom = shape(geojson['features'][0]['geometry'])
min_date = datetime.date(2016, 1, 30) 


if __name__ == '__main__':
    generator = BuildingsGenerator(geom.wkt, min_date)
    buildings = generator.generate()
    print buildings.head()
