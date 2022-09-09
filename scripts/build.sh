#!/usr/bin/env bash

BASE=`dirname $0`

cd $BASE/..

rm -rf build_tmp
mkdir build_tmp

echo "Install custom hdt-java fixed version"

git clone -b diff_fix http://github.com/ate47/hdt-java build_tmp/hdt-java

cd build_tmp/hdt-java

mvn install -DskipTests

cd ../..

rm -rf build_tmp

./gradlew --refresh-dependencies

./gradlew shadowJar

echo "Compiled, writing to wiki-changes.jar"

cp "./build/libs/wikidata-changes-$(scripts/get_version.sh)-all.jar" wiki-changes.jar

