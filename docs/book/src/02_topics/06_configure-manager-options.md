## Supported Manager Options

At present, the following feature gates and flags are supported for configuration.

### Feature Gates

| Name            | Description                                           | Default | Env Variable     |
|-----------------|-------------------------------------------------------|---------|------------------|
| MachinePool     | for MachinePool functionality                         | true    | EXP_MACHINE_POOL |
| ClusterTopology | for ClusterClass and managed topologies functionality | true    | CLUSTER_TOPOLOGY |
| MachineTaintPropagation | Propagate control-plane Machine taints to Nodes | false | EXP_MACHINE_TAINT_PROPAGATION |

### Flags

| Name         | Description                                                                        | Default | Env Variable       |
|--------------|------------------------------------------------------------------------------------|---------|--------------------|
| concurrency  | Number of core resources to process simultaneously                                 | 10      | CONCURRENCY_NUMBER |
| watch-filter | Value of the `cluster.x-k8s.io/watch-filter` label of the objects to reconcile     | ""      | -                  |

## Configuring Manager Options
In order to configure the manager options, it is required to patch the respective values in the 
`rke2-bootstrap-controller-manager` and `rke2-control-plane-controller-manager` deployments.

- Enable/Disable the feature flags:
> **Note:** `MachineTaintPropagation` must be enabled in both the Cluster API and RKE2 control-plane controller managers. See [Taint propagation](./10_taint_propagation.md).
```shell
$ kubectl -n system edit deployment/rke2-bootstrap-controller-manager 

// Enable/disable available features by modifying args below.
    args:
      ...
    - "--feature-gates=MachinePool=true,ClusterResourceSet=true"
```

- Configure the concurrency options for manager:
> **Note:** Configuring manager options is supported for `rke2-bootstrap-controller-manager` as well as `rke2-control-plane-controller-manager`.

- Update `concurrency` arg:
```shell
$ kubectl -n system edit deployment/controller-manager

// Modify the value of  --concurrency  in args
   args:
      ...
    - "--concurrency=<required_value>"
```

The patch shall rollout the deployment, and should spin up new pods with updated values.

- Restrict the objects reconciled by the manager, by adding the `watch-filter` arg to **both** the
`rke2-bootstrap-controller-manager` and the `rke2-control-plane-controller-manager` deployments:
```shell
$ kubectl -n rke2-control-plane-system edit deployment/rke2-control-plane-controller-manager
$ kubectl -n rke2-bootstrap-system edit deployment/rke2-bootstrap-controller-manager

// Add  --watch-filter  to args
   args:
      ...
    - "--watch-filter=<required_value>"
```

When `--watch-filter` is set, the controllers only process events of objects carrying the
`cluster.x-k8s.io/watch-filter: <required_value>` label, which allows running multiple instances of the
provider in the same management cluster, each one taking care of a different set of clusters.

> **Note:** An instance running without `--watch-filter` reconciles every object, labeled ones included.
> Every instance sharing a management cluster must therefore have its own filter value, otherwise the same
> objects are reconciled by more than one instance.

> **Note:** The managers use leader election with a fixed lease name in their own namespace, so a second
> instance deployed in the same namespace never becomes the leader and reconciles nothing. Install every
> instance in its own namespace (`clusterctl` cannot install a provider twice, so the additional instances
> have to be deployed by other means), or run them with `--leader-elect=false`.

The label must be set, with the same value, on:
- the `Cluster`;
- the `RKE2ControlPlane` and its `spec.machineTemplate.metadata.labels`, from where it is propagated to the
  control plane `Machines`, their infrastructure machines and their `RKE2Configs`;
- the `MachineDeployments` and `MachinePools`, and their `spec.template.metadata.labels`, from where it is
  propagated to the worker `Machines` and, for `MachineDeployments`, to their `RKE2Configs`;
- the `RKE2Config` referenced by a `MachinePool`, which Cluster API does not label from the template.

With `ClusterClass`, the labels are set through the `metadata` of `spec.topology.controlPlane` and of each
`spec.topology.workers` entry.

The label of the `Cluster` or of the `RKE2ControlPlane` object itself is not propagated to any other object.
Objects without the label are ignored by the instances that have `--watch-filter` set. Labeling the
`RKE2ControlPlane` template or a `MachineDeployment` template of an existing cluster propagates the label to
its existing `Machines`, infrastructure machines and `RKE2Configs` in place.

> **Troubleshooting:** if the `RKE2ControlPlane` is labeled but its `Cluster` is not, the instance does not
> connect to the workload cluster: the control plane reports `connection to the workload cluster is down`
> errors and never becomes initialized.
