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
    if [[ $DELAY < 5 ]]; then DELAY=5; fi
    sleep $DELAY
done
