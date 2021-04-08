IMAGE_NAME=tybalex/drainmodel
s2i build . seldonio/seldon-core-s2i-python3:1.7.0-dev $IMAGE_NAME

docker push $IMAGE_NAME
