FROM developmentseed/geolambda:min

# install app
COPY . /build
WORKDIR /build
RUN \
    yum install git; \
    pip install Cython; \
    pip install -r /build/requirements.txt; \
    rm -rf /build/*; \

