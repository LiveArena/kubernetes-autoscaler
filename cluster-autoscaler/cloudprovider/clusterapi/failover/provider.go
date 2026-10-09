package failover

// MachinePoolKind and related constants identify supported resources and sizing metadata.
const (
	MachinePoolKind      = "MachinePool"
	AzureAPIGroup        = "infrastructure.cluster.x-k8s.io"
	MaxSizeAnnotationKey = "cluster.x-k8s.io/cluster-api-autoscaler-node-group-max-size"
)
