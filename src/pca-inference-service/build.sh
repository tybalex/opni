IMAGE=tybalex/seldon-window-inference-controller
docker build .. -t $IMAGE_NAME -f ./Dockerfile
docker push $IMAGE
