# Run GAE in Job Mode

Instead of started as a server, GAE could also be executed in batch style, we leverage [mpi-operator](https://github.com/kubeflow/mpi-operator?tab=readme-ov-file) to launch a GAE job.

Usage:

Make sure mpi-operator is installed, then create a Job by using this [yaml yaml](https://github.com/alibaba/GraphScope/blob/main/examples/analytical/mpi-operator/wcc.yaml).

```bash
kubectl create -f https://github.com/alibaba/GraphScope/blob/main/examples/analytical/mpi-operator/wcc.yaml
```

You can see a master pod and several worker pod has been created, and GAE automatically exits after the job completed.
