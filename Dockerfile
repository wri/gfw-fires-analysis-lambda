# Dockerfile.base

FROM lambdalinux/baseimage-amzn:2017.03-004

RUN \
	yum -y update; \
	# python 2.7
	yum install -y rsync zip git gcc python27-devel python27-pip; \
        pip install Cython; \
    yum clean all;

ENV \
    LANG=en_US.UTF-8 \
    LC_ALL=en_US.UTF-8
COPY . /build
WORKDIR /build
RUN \
  pip install -r /build/requirements.txt; \
  rm -rf /build/*; \
