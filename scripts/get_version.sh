#!/usr/bin/env bash

BASE=`dirname $0`

cd $BASE/..

VERSION="$(./gradlew printVersion -q)"

echo $VERSION