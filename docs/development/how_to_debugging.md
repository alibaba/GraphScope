This document shows how to debugging GraphScope under various conditions.

### Debugging on local deployment


### Debugging on Kubernetes deployment

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
