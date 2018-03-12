FROM developmentseed/geolambda:core

# GDAL2
RUN \
        yum install -y sqlite.x86_64 sqlite-devel.x86_64 && \
	wget http://download.osgeo.org/gdal/$GDAL_VERSION/gdal-$GDAL_VERSION.tar.gz && \
	tar -xzvf gdal-$GDAL_VERSION.tar.gz && \
	cd gdal-$GDAL_VERSION && \
    ./configure --prefix=$PREFIX \
        --without-python \
        --with-hdf4=no \
        --with-hdf5=no \
        --with-threads \
        --with-gif=no \
        --with-pg=no \
        --with-grass=no \
        --with-libgrass=no \
        --with-cfitsio=no \
        --with-pcraster=no \
        --with-netcdf=no \
        --with-png=no \
        --with-jpeg=no \
        --with-gif=no \
        --with-ogdi=no \
        --with-fme=no \
        --with-jasper=no \
        --with-ecw=no \
        --with-kakadu=no \
        --with-mrsid=no \
        --with-jp2mrsid=no \
        --with-bsb=no \
        --with-grib=no \
        --with-mysql=no \
        --with-ingres=no \
        --with-xerces=no \
        --with-expat=no \
        --with-odbc=no \
        --with-curl=yes \
        --with-sqlite3=yes \
        --with-dwgdirect=no \
        --with-idb=no \
        --with-sde=no \
        --with-perl=no \
        --with-php=no \
        --without-mrf \
        --with-hide-internal-symbols=yes \
        CFLAGS="-O2 -Os" CXXFLAGS="-O2 -Os" && \ 
        make; make install; cd swig/python; \
        python setup.py install; \
        python3 setup.py install; \
        cd $BUILD; rm -rf gdal-$GDAL_VERSION*

# install app
COPY . /build
WORKDIR /build
RUN \
    yum install git; \
    pip install Cython; \
    pip install -r /build/requirements.txt; \
    pip install . -v; \
    rm -rf /build/*; \
    # Manually enable reading of VRT - disabled by default
    sed -i 's/#   ("VRT"/   ("OGR_VRT"/g' /usr/local/lib64/python2.7/site-packages/fiona/drvsupport.py; \
    sed -i 's/("GPKG", "rw")/("GPKG", "raw")/g' /usr/local/lib64/python2.7/site-packages/fiona/drvsupport.py;
   

WORKDIR /home/geolambda
