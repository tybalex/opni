# seldon core for AIOps

## Steps to replicate setup Seldon core and customized NuLog server.

prerequisites: Helm3, kubectl, a kubernetes cluster, python3.6+(optional).

### TLDR; one script installation for testing

```
bash end-to-end-testing.sh
```
and uninstall
```
bash uninstall-end-to-end-testing.sh
```

### install seldon-core
```
## (optional): install ambassador
kubectl create namespace ambassador
helm repo add datawire https://www.getambassador.io
helm repo update
helm install ambassador datawire/ambassador \
    --set image.repository=quay.io/datawire/ambassador \
    --set enableAES=false \
    --set crds.keep=false \
    --namespace ambassador

kubectl rollout status deployment.apps/ambassador -n ambassador

## to port forward ambassador:
kubectl port-forward $(kubectl get pods -n ambassador -l app.kubernetes.io/name=ambassador -o jsonpath='{.items[0].metadata.name}') -n ambassador 8003:8080
```
```
## install seldon core
## reference: https://docs.seldon.io/projects/seldon-core/en/latest/examples/seldon_core_setup.html#Setup-Cluster
kubectl create namespace seldon-system
helm install seldon-core seldon-core-operator \
    --repo https://storage.googleapis.com/seldon-charts \
    --set usageMetrics.enabled=true \
    --namespace seldon-system \
    # --set ambassador.enabled=true ## add this line if you have istio installed

kubectl rollout status deploy/seldon-controller-manager -n seldon-system
```

### (OPTIONAL): package models and push to docker repository
```## a bash script for build all
bash buildall.sh
```
```
# Optional: try out the wrapped server locally:
cd pca-server
docker build . -t pcaserver
## docker run the server
docker run -p 9000:9000 pcaserver

## send a payload for inference
curl -d '{"jsonData": ["testing sentence for inference!", "hello helllo", "123 567"]}' \
   -X POST http://localhost:9000/api/v1.0/predictions \
   -H "Content-Type: application/json"

## expected results:
{"data":{"names":[],"tensor":{"shape":[3],"values":[0.625,0.5,0.5]}},"meta":{}}
```

<!-- 3. update the configmap seldon-config in seldon-system, this tells it where to pull our new server image.
edit it directly (Aka what has been added):
```
kubectl edit  configmap seldon-config  -n seldon-system
<<<<<<< HEAD
# in predictor_servers, add this line at beginning
"PCASERVER":{"protocols":{"seldon":{"defaultImageVersion":"latest","image":"tybalex/pcaserver"}}},
# basically, PCASERVER is the name of our custom server, image is the image we've just pushed
``` -->

### Install NVIDIA GPU driver
`kubectl apply -f https://raw.githubusercontent.com/NVIDIA/k8s-device-plugin/v0.6.0/nvidia-device-plugin.yml`

### start the model server and test!
``` ## start model serve
kubectl create namespace seldon
kubectl apply -f nulog+drain-inference-service.yaml
```

``` ## test. ref: https://docs.seldon.io/projects/seldon-core/en/v1.1.0/workflow/serving.html#testing-your-model-on-kubernetes
## simply testing by port forward:
kubectl port-forward svc/aiops-default -n seldon 8000:8000
curl -d '{"jsonData": {"log":["testing sentence for inference!", "hello helllo", "123 567"], "window":0}}' \
   -X POST http://localhost:8000/api/v1.0/predictions \
   -H "Content-Type: application/json"

## if ambassador is installed, this should also be tested through the end point `http://<ingress_url>/seldon/<namespace>/<model-name>/api/v1.0/predictions`, in our case:

## get ambassador endpoint
export AMBASSADOR_ENDPOINT=$(kubectl -n ambassador get svc ambassador -o "go-template={{range .status.loadBalancer.ingress}}{{or .ip .hostname}}{{end}}")
curl -d '{"jsonData": {"log":["testing sentence for inference!", "hello helllo", "123 567"], "window":0}}' \
   -X POST http://$AMBASSADOR_ENDPOINT/seldon/seldon/aiops/api/v1.0/predictions \
   -H "Content-Type: application/json"

## or port forward ambassador to localhost:
kubectl port-forward $(kubectl get pods -n ambassador -l app.kubernetes.io/name=ambassador -o jsonpath='{.items[0].metadata.name}') -n ambassador 8003:8080
curl -d '{"jsonData": {"log":["testing sentence for inference!", "hello helllo", "123 567"], "window":0}}' \
   -X POST 0.0.0.0:8003/seldon/seldon/aiops/api/v1.0/predictions \
   -H "Content-Type: application/json"

## test with python seldon client: (ref: https://docs.seldon.io/projects/seldon-core/en/latest/python/api/seldon_core.html#seldon_core.seldon_client.SeldonClient)
python python_client.py
```

## Next Steps
1. build our custom server with nvidia's triton server https://github.com/triton-inference-server/server


3. use the `Alibi` python package to create explainer for models, so it can help explain the predictions. (ref: https://github.com/SeldonIO/alibi)

4. a more complex inference graph for serving multiple models

