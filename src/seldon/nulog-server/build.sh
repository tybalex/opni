IMAGE_NAME=tybalex/nulogmodel
docker build . -t $IMAGE_NAME

docker push $IMAGE_NAME

