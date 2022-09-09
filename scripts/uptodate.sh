#!/usr/bin/env bash
BASE="$(dirname $0)/.."

## Config

# Java options
JAVA_OPTIONS=-Xmx32g
# Loop time, will start the process every $LOOP seconds, won't restart it if the previous iteration didn't complete
LOOP=$((24 * 3600))
# RDF flavor: "full" or "simple" (for truthy dataset)
FLAVOR=full
# Start date file, should contain an UTC date with the start date
START_DATE_FILE=$BASE/date.txt
# Java executable
JAVA=java
# Wiki-Changes jar
JAR=$BASE/wiki-changes.jar


## Usage

if (( $# < 2 )); then
    echo "$0 (base hdt) (output hdt)" >&2
    echo "  base hdt        : HDT to start the process, will be overwritten after every loop"
    echo "  output hdt      : HDT write at the end of the process"
    
    exit -1
fi

## Read option

BASE_HDT=$1
OUTPUT_HDT=$2

if ! [ -f "$START_DATE_FILE" ]; then
    >&2 echo "File \"$START_DATE_FILE\" doesn't exist, you should create one with the date you want to look back for the update."
    exit -1
fi

if ! [ -f "$BASE_HDT" ]; then
    >&2 echo "File \"$BASE_HDT\" doesn't exist, you should create one with the base HDT."
    exit -1
fi

while true; do
    START=$(date +%s)

    # Compute the new index

    CURRENT_DATE=$($JAVA -jar "$JAR" -T)
    TO_DATE=$(cat $START_DATE_FILE)
    
    # Execute the updater
    $JAVA $JAVA_OPTIONS -jar "$JAR" --date $TO_DATE --hdtsource $BASE_HDT --flavor $FLAVOR

    # Create a backup of the base HDT
    mv $BASE_HDT "$BASE_HDT.backup"

    # Write the result with the index
    cp cache/result.hdt $OUTPUT_HDT
    mv cache/result.hdt.index.v1-1 "$OUTPUT_HDT.index.v1-1"
    # Replace the base and set the new date
    mv cache/result.hdt $BASE_HDT
    echo "$CURRENT_DATE" > $START_DATE_FILE

    # Delete the backup
    rm "$BASE_HDT.backup"

    END=$(date +%s)

    
    DELTA=$(echo "$END - $START" | bc)
    echo "computed delta in $DELTA seconds"

    if (( $LOOP != 0 )); then
        # Waiting for the next loop to run if required
        PARTIAL_WAIT=$(echo "$LOOP - $DELTA" | bc)

        if (( $PARTIAL_WAIT < 0 )); then
            >&2 echo "The delta computation tooks more than $LOOP seconds! ($(($DELTA))s)" 
        else
            sleep $PARTIAL_WAIT
        fi
    fi
done
