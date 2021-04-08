IMAGE=tybalex/seldon-window-inference-controller
docker build . -t $IMAGE
docker push $IMAGE
