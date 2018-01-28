FROM developmentseed/geolambda:min

# install app
COPY . /build
WORKDIR /build
RUN yum install git
RUN pip install Cython
RUN \
    pip install -r /build/requirements.txt; \
    pip install . -v; \
    rm -rf /build/*;

WORKDIR /home/geolambda
