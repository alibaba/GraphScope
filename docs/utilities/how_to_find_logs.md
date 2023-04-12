# How to find logs

By default, GraphScope is usually running in a silent mode following the convention of Python applications. To enable verbose logging, turn on by:

```python
import graphscope

graphscope.set_option(show_log=True)
```

If you are running GraphScope in k8s, you can use [kubectl describe](https://kubernetes.io/docs/reference/generated/kubectl/kubectl-commands#describe) and [kubectl logs](https://kubernetes.io/docs/reference/generated/kubectl/kubectl-commands#logs) to check the log/status of the cluster. If the disk storage is accessible(on local or via Pods), you may also find logs in `/tmp/gs/runtime/logs`.

## Groot

It is common to find the logs of Frontend and Store roles. When debugging, it is often necessary to find the logs of Coordinator and Ingestor as well. The logs of Frontend include the logs of the Compiler that generates the logical query plan, while the logs of Store include the logs of the query engine execution. You can find the logs of each Pod using the [kubectl command](https://kubernetes.io/docs/reference/generated/kubectl/kubectl-commands) `kubectl logs ${POD_NAME}`. For example,

```bash
kubectl logs demo-graphscope-store-frontend-0
kubectl logs demo-graphscope-store-store-0
```
