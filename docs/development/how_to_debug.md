This document shows how to debugging GraphScope under various conditions.

### Debugging on local deployment

## Find the logs

Most of the logs will be streamed through the stdout of client, you could control the log level by 

```python
import graphscope
graphscope.set_option(show_log=True)
graphscope.set_option(log_level='DEBUG')  # could also be INFO, ERROR
```

As you may know, GraphScope is composed of three engines, where the detailed log location of each engine is

- Analytical Engine: `/tmp/grape_engine.INFO`
- Interactive Engine: Inside `/var/log/graphscope/` or `$HOME/log/graphscope` if GraphScope doesn't have permission of `/var/log`. You may find several folders named with a long number, which is the object id of the graph. There is also a `current` folder links to the log folder of latest created interactive instance.
- Learning Engine: `graphlearn.INFO` in the current directory.



### Debugging on Kubernetes deployment

## Find the logs

In kubernetes environment, besides most of the logs still output to console, you could find detailed logs in each pod's stdout, or files inside each pods.

Note: You could use `kubectl logs <pod>` to inspect the stdout of the pod. Use `kubectl logs <pod> -c <container>` to inspect a specific container inside the pod.

- Coordinator: The stdout of coordinator pod.
- Analytical Engine: The stdout engine container in the engine pod.
- Interactive Engine: The stdout of executor container in the engine pod for the executor log. And the stdout of interactive-frontend pod for the frontend. The log files resides in the `/var/log/graphscope` of each container, respectively.


## Commands for Debugging

Here is list with commands usually used for checking the status of the GraphScope deployment on K8s.

- Check the status of the pods in a specific namespace `kubectl get pods -n <namespace>`
- Get detailed information about a pod: `kubectl describe pod <pod-name> -n <namespace>`
- Check the logs of a container in a pod: `kubectl logs <pod-name> <container-name> -n <namespace>`
- Check the events related to a pod: `kubectl get events --field-selector involvedObject.name=<pod-name> -n <namespace>`
- Check the configuration of a resource: `kubectl get <resource-type> <resource-name> -o yaml -n <namespace>`

### Debugging Techniques
- Verify that the Docker image used in the deployment is correct and can be pulled by Kubernetes.
- Check the logs of the container in the pod to see if there are any error messages or warnings that could indicate the source of the problem.
- Increase the log level of the GraphScope components. This will produce more verbose output that may help pinpoint the source of the problem.
    ```python
    import graphscope
    graphscope.set_option(show_log=True)
    graphscope.set_option(log_level='DEBUG')
    ```
- Use kubectl exec to access a running container in a pod and run diagnostic commands inside it.
- Use kubectl port-forward to forward a port from a pod to your local machine and access the service directly from your browser or command line.
- Check the Kubernetes events to see if there are any events related to the pod that could explain the problem.
