IMAGE_NAME=tybalex/pcamodel
docker build . -t $IMAGE_NAME

docker push $IMAGE_NAME
