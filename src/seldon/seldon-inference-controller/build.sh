IMAGE=tybalex/seldon-inference-controller
docker build . -t $IMAGE
docker push $IMAGE
