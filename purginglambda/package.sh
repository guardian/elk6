#!/bin/bash

if [ ! -d "$PWD/package" ]; then mkdir "$PWD/package"; fi

cp src/*.py package/

#cp -r ${VIRTUAL_ENV}/lib/python3.7/site-packages/ package
cd package
pip install -r ../requirements.txt --target .
cd package
chmod -R 777 *

zip -r9 ../function.zip .
