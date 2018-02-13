#!/bin/bash

# directory used for deployment
DEPLOY_DIR=lambda
PREFIX=/usr/local

PYVER=${1:-2.7}

# make deployment directory and add lambda handler
mkdir -p $DEPLOY_DIR/lib/python$PYVER/site-packages

# copy 32-bit libs
rsync -ax $PREFIX/lib/python$PYVER/site-packages/ $DEPLOY_DIR/lib/python$PYVER/site-packages/ --exclude-from etc/lambda-excluded-packages

#cp /usr/lib64/libpq.so.5 $DEPLOY_DIR/lib/
rsync -ax $PREFIX/lib64/python$PYVER/site-packages/ $DEPLOY_DIR/lib/python$PYVER/site-packages/ --exclude-from etc/lambda-excluded-packages

# zip up deploy package
cd $DEPLOY_DIR
zip -ruq ../lambda-deploy.zip ./

# remove lib dir because we are not using when developing locally
rm -r lib/
