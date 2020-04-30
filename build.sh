#!/bin/bash

set -e

IMAGE=cvm-fund-explorer-workers

docker build -t $IMAGE .

if [ -n "$TRAVIS_BRANCH" ]; then
    echo "$DOCKER_PASSWORD" | docker login -u "$DOCKER_USERNAME" --password-stdin
    docker tag $IMAGE $DOCKER_USERNAME/$IMAGE:${TRAVIS_BRANCH//[\/ ]/_}
    docker push $DOCKER_USERNAME/$IMAGE:${TRAVIS_BRANCH//[\/ ]/_}

    if [ "$TRAVIS_BRANCH" = "master" ]; then        
        docker tag $IMAGE $DOCKER_USERNAME/$IMAGE:latest
        docker push $DOCKER_USERNAME/$IMAGE:latest
    fi
else
    BRANCH=$(git rev-parse --abbrev-ref HEAD)
    docker tag $IMAGE conradoqg/$IMAGE:${BRANCH//[\/ ]/_}
    if [ "$BRANCH" = "master" ]; then
        docker tag $IMAGE conradoqg/$IMAGE:latest
    fi
fi

docker images