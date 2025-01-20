# Deploy GraphScope Operator


## Coordinator configuration

Save and modify the following configuration template.

```yaml
coordinator:
  service_port: 63800

launcher_type: operator

operator_launcher:
  gae_endpoint: 'gs-engine:60001'  # gae endpoint, to be resolved by gRPC. Could be a service endpoint.
  hosts: ["gs-engine-0", "gs-engine-1"]  # pod name lists
  namespace: default  # namespace

session:
  num_workers: 2  # number of engine workers (pods)
  instance_id: instance-0000  # A random instance id

vineyard:
  socket: /tmp/vineyard.sock  # use the actual socket path
  rpc_port: 9600
```

Say the configuration has been saved in `/home/graphscope/config.yaml`, then start the coordinator with the config file.

```python
python3 -m gscoordinator --config-file /home/graphscope/config.yaml
```

In practice, the configuration file should be saved to a configmap, and mount that configmap to the pod of coordinator.
