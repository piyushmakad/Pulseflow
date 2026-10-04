# Deploy PulseFlow to Oracle OKE

This profile runs PulseFlow on a single-node OKE Basic cluster. Prefer an
Always Free `VM.Standard.A1.Flex` node when capacity exists. A paid x86 Flex
shape can be used temporarily with Free Trial credits when A1 capacity is
unavailable. This setup is suitable for learning, but it is not highly
available.

## 1. Create Oracle resources

In the OCI Console:

1. Create a `pulseflow` compartment in the tenancy home region.
2. Create an OKE Basic cluster.
3. Select a public Kubernetes API endpoint and private managed workers.
4. For Always Free, select `VM.Standard.A1.Flex`, one node, 2 OCPUs and 12 GB
   memory in the tenancy home region. If Oracle reports
   `OUT_OF_HOST_CAPACITY`, wait for capacity or knowingly use a paid trial
   shape such as `VM.Standard.E3.Flex`.
5. Keep the boot volume at 50 GB.
6. Attach the generated worker NSG to the node pool.
7. Create a private OCI Container Registry repository named `pulseflow`.

Do not select an Enhanced cluster if the goal is to avoid the paid OKE control
plane. Non-A1 worker shapes consume trial credits or incur charges on a paid
account.

## 2. Configure kubectl

Open the cluster in OCI, choose **Access cluster**, and run the provided OCI CLI
command. Verify the selected context:

```bash
kubectl config current-context
kubectl get nodes
kubectl get nodes -o jsonpath='{.items[*].status.nodeInfo.architecture}'
```

The node architecture is `arm64` for A1 workers and `amd64` for E3/E5/E6
workers. The PulseFlow container is published for both architectures.

## 3. Registry path

The Oracle overlays use this Mumbai OCIR repository:

```text
bom.ocir.io/bm7wpbkcaaqu/pulseflow
```

Change both overlay `kustomization.yaml` files if the tenancy namespace or
region changes.

## 4. Build and push the image

Create an OCI auth token and log in. OCI usernames commonly use
`tenancy-namespace/username`:

```bash
docker login bom.ocir.io
make docker-push-ocir
```

This publishes both ARM64 and AMD64 variants under the same version tag.

## 5. Create namespace and secrets

```bash
make k8s-namespace
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
make k8s-infra
kubectl get pvc -n pulseflow
```

Both persistent volume claims must be `Bound` before continuing.

## 7. Run migrations

Job templates contain immutable fields, so remove the previous completed Job
before creating the next release's migration Job:

```bash
make k8s-migrate
```

Do not deploy the application if this Job fails.

## 8. Deploy API and workers

```bash
make k8s-app
make k8s-status
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
