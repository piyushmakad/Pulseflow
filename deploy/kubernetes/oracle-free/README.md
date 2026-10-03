# Deploy PulseFlow to Oracle OKE Free

This profile runs PulseFlow on an OKE Basic cluster backed by an Always Free
`VM.Standard.A1.Flex` ARM64 node. It is suitable for learning and a persistent
demo, but it is not highly available.

## 1. Create Oracle resources

In the OCI Console:

1. Create a `pulseflow` compartment in the tenancy home region.
2. Create an OKE cluster using **Quick Create**.
3. Select a public Kubernetes API endpoint and private managed workers.
4. Select `VM.Standard.A1.Flex`, one node, 4 OCPUs and 24 GB memory.
5. Keep the boot volume at 50 GB.
6. On the final screen, explicitly choose **Create a Basic cluster**.
7. Create a private OCI Container Registry repository named `pulseflow`.

Do not select an Enhanced cluster or non-A1 worker shape if the goal is to stay
inside the free allowances.

## 2. Configure kubectl

Open the cluster in OCI, choose **Access cluster**, and run the provided OCI CLI
command. Verify the selected context:

```bash
kubectl config current-context
kubectl get nodes
kubectl get nodes -o jsonpath='{.items[*].status.nodeInfo.architecture}'
```

The node architecture should be `arm64`.

## 3. Set the registry path

Edit both files:

```text
deploy/kubernetes/oracle-free/app/kustomization.yaml
deploy/kubernetes/oracle-free/migration/kustomization.yaml
```

Replace:

```text
bom.ocir.io/replace-with-tenancy-namespace/pulseflow
```

with the actual repository. `bom.ocir.io` is the Mumbai endpoint; use the OCI
endpoint for the tenancy's home region if it differs.

## 4. Build and push the image

Create an OCI auth token and log in. OCI usernames commonly use
`tenancy-namespace/username`:

```bash
docker login bom.ocir.io
docker buildx build --platform linux/amd64,linux/arm64 \
  -t bom.ocir.io/TENANCY_NAMESPACE/pulseflow:0.1.0 --push .
```

The ARM64 variant runs on Ampere; AMD64 remains usable locally.

## 5. Create namespace and secrets

```bash
kubectl apply -f deploy/kubernetes/oracle-free/namespace.yaml
```

Create the registry pull secret without storing its auth token in Git:

```bash
kubectl create secret docker-registry ocir-pull-secret \
  --namespace pulseflow \
  --docker-server=bom.ocir.io \
  --docker-username='TENANCY_NAMESPACE/USERNAME' \
  --docker-password='OCI_AUTH_TOKEN'
```

Copy and edit the application secret:

```bash
cp deploy/kubernetes/oracle-free/secret.example.yaml \
   deploy/kubernetes/oracle-free/secret.yaml
```

Replace every placeholder, then apply it:

```bash
kubectl apply -f deploy/kubernetes/oracle-free/secret.yaml
```

`secret.yaml` is gitignored. Special characters in URL passwords must be
URL-encoded.

## 6. Deploy stateful infrastructure

```bash
kubectl apply -k deploy/kubernetes/oracle-free/infra
kubectl rollout status statefulset/postgres -n pulseflow --timeout=5m
kubectl rollout status statefulset/kafka -n pulseflow --timeout=10m
kubectl rollout status deployment/redis -n pulseflow --timeout=5m
kubectl wait --for=condition=complete job/kafka-topics \
  -n pulseflow --timeout=10m
kubectl get pvc -n pulseflow
```

Both persistent volume claims must be `Bound` before continuing.

## 7. Run migrations

Job templates contain immutable fields, so remove the previous completed Job
before creating the next release's migration Job:

```bash
kubectl delete job pulseflow-migrate -n pulseflow --ignore-not-found
kubectl apply -k deploy/kubernetes/oracle-free/migration
kubectl wait --for=condition=complete job/pulseflow-migrate \
  -n pulseflow --timeout=5m
kubectl logs job/pulseflow-migrate -n pulseflow
```

Do not deploy the application if this Job fails.

## 8. Deploy API and workers

```bash
kubectl apply -k deploy/kubernetes/oracle-free/app
kubectl rollout status deployment/pulseflow-api -n pulseflow --timeout=5m
kubectl rollout status deployment/pulseflow-worker -n pulseflow --timeout=5m
kubectl get pods -n pulseflow
```

## 9. Get the API address

```bash
kubectl get service pulseflow-api -n pulseflow
```

OCI can take several minutes to assign the external address. Then test:

```bash
curl http://EXTERNAL_IP/healthz
curl http://EXTERNAL_IP/readyz
```

Add TLS before transmitting real credentials or customer events publicly.

## 10. Provision the first tenant

Run the provisioning command inside one API pod so it inherits the already
configured ConfigMap and Secret environment:

```bash
kubectl exec deployment/pulseflow-api -c pulseflow -n pulseflow -- \
  /app/pulseflow --command=provision \
  --tenant-name='Oracle Demo' --key-name=default
```

Record the API key when shown; its raw value is displayed once.

## Scaling workers

```bash
kubectl scale deployment/pulseflow-worker --replicas=2 -n pulseflow
```

Two pods with `WEBHOOK_WORKER_COUNT=4` allow up to eight concurrent webhook
handlers. Scale slowly: each pod also owns a PostgreSQL pool, Kafka consumer,
outbox relay and delivery buffers.

## Diagnostics

```bash
kubectl get all -n pulseflow
kubectl logs deployment/pulseflow-api -n pulseflow
kubectl logs deployment/pulseflow-worker -n pulseflow
kubectl describe pod POD_NAME -n pulseflow
kubectl get events -n pulseflow --sort-by=.lastTimestamp
```

## Data warning

Never delete these persistent claims merely to retry a deployment:

```text
data-postgres-0
data-kafka-0
```

PostgreSQL contains authoritative PulseFlow state. Establish and test backups
before treating this environment as anything beyond a learning system.
