#!/bin/bash -x

while :

do
    SECONDS=0

    scrapy crawl divulga

    pushd data/download/oficial
    git add .
    git commit -m "Updated state"
    popd

    echo "Took $SECONDS"
    DELAY=$((10 - SECONDS))
    if [[ $DELAY < 2 ]]; then DELAY=2; fi
    sleep $DELAY
done
