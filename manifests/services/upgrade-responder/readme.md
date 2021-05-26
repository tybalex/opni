### Client End
kubectl apply -f up-client.yaml

### Upgrade-Responder Server End

- Prepare a cluster (EKS)
- Install InfluxDB v1.8:  `helm install influxdb influxdata/influxdb`
- `helm upgrade --install opni-upgrade-responder ./chart -f opni-upgrade-responder.yaml`
- Expose service opni-upgrade-responder to a loadbalancer or simple port-forward to localhost
  example: `kubectl port-forward svc/opni-upgrade-responder 8314:8314`
- quick test: 
   ```
   curl -X POST http://localhost:8314/v1/checkupgrade \
     -d '{ "appVersion": "v0.0.1", "extraInfo": {}}'
   ```
- grafana: `helm install grafana grafana/grafana --set persistence.enabled=true`
- get grafana password `kubectl get secret --namespace default grafana -o jsonpath="{.data.admin-password}" | base64 --decode ; echo`
- create the grafana dashboard: https://github.com/longhorn/upgrade-responder#2-creating-grafana-dashboard


Ref: https://github.com/longhorn/upgrade-responder#3-modifying-your-application
And https://github.com/longhorn/upgrade-responder/tree/master/chart