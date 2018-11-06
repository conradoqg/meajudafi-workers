#!/bin/sh

IMAGE=cvm-fund-explorer-workers

echo "$DOCKER_PASSWORD" | docker login -u "$DOCKER_USERNAME" --password-stdin
docker build -t $IMAGE .

docker tag $IMAGE $DOCKER_USERNAME/$IMAGE:$TRAVIS_BRANCH
docker push $DOCKER_USERNAME/$IMAGE:$TRAVIS_BRANCH

if [ "$TRAVIS_BRANCH" = "master" ]; then
    docker tag $IMAGE $DOCKER_USERNAME/$IMAGE:latest
    docker push $DOCKER_USERNAME/$IMAGE:latest
fi

docker images