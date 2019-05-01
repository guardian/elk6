#!/usr/bin/env bash -e

if [ "$1" == "" ]; then
  echo You must specify the bucket as the first parameter
  exit 1
fi

if [ "$2" == "" ]; then
  echo You must specify the stage \(in caps\) as the second parameter
  exit 1
fi

if [ -f function.zip ]; then rm -f function.zip; fi

./package.sh
aws s3 cp function.zip s3://$1/elk-purging-lambda/$2/function.zip
