helm uninstall nats;
helm uninstall elasticsearch;
#helm uninstall traefik;

kubectl delete -f preprocessing.yaml
kubectl delete -f fastapi-fluentd-sink.yaml

kubectl delete -f inference-controller.yaml

# kubectl delete -f start-inference-service.yaml
kubectl delete -f nulog+drain-inference-service.yaml
kubectl delete -f https://raw.githubusercontent.com/NVIDIA/k8s-device-plugin/v0.6.0/nvidia-device-plugin.yml

helm uninstall seldon-core -n seldon-system