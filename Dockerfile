FROM developmentseed/geolambda:min

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
    sed -i 's/#   ("VRT"/   ("OGR_VRT"/g' /usr/local/lib64/python2.7/site-packages/fiona/drvsupport.py

WORKDIR /home/geolambda
