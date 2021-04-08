kubectl create namespace seldon-system
helm install seldon-core seldon-core-operator \
    --repo https://storage.googleapis.com/seldon-charts \
    --set usageMetrics.enabled=true \
    --namespace seldon-system

helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo add traefik https://helm.traefik.io/traefik
helm repo update

helm install nats --set auth.enabled=true,auth.password=VfU6TcAl9x,replicaCount=3,maxPayload=10485760 bitnami/nats
helm install elasticsearch --set global.kibanaEnabled=true bitnami/elasticsearch --render-subchart-notes
#helm install traefik traefik/traefik

kubectl apply -f fastapi-fluentd-sink.yaml
kubectl apply -f preprocessing.yaml

kubectl create namespace seldon
kubectl apply -f https://raw.githubusercontent.com/NVIDIA/k8s-device-plugin/v0.6.0/nvidia-device-plugin.yml
# kubectl apply -f start-inference-service.yaml
kubectl apply -f nulog+drain-inference-service.yaml

kubectl apply -f inference-controller.yaml

kubectl  patch svc elasticsearch-kibana -p '{"spec": {"type": "LoadBalancer"}}'

export ENDPOINT=`kubectl get svc traefik -o jsonpath='{.status.loadBalancer.ingress[*].hostname}'`;
echo $ENDPOINT;