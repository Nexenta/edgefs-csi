# CSI driver for EdgeFS (1.2.0 spec conformance)

[Container Storage Interface (CSI)](https://github.com/container-storage-interface/) driver, provisioner,attacher and snapshotter for EdgeFS Scale-Out NFS and ISCSI services

## Overview

EdgeFS CSI plugins implement an interface between CSI enabled Container
Orchestrator (CO) and EdgeFS local cluster site. It allows dynamic and
static provisioning of EdgeFS NFS exports and iSCSI LUNs.

With EdgeFS NFS implementation, I/O load can be spread-out across
multiple PODs, thus eliminating I/O bottlenecks of single-node NFS.

With EdgeFS iSCSI implementation, I/O load scales out across all EdgeFS
nodes, thus providing highly available and performant solution.

Current implementation of EdgeFS CSI plugins was tested in Kubernetes
environment (requires Kubernetes 1.13+)

For details about configuration and deployment of EdgeFS CSI plugin,
see Wiki pages:

* [Quick Start Guide](https://github.com/Nexenta/edgefs-csi/wiki/EdgeFS-CSI-Quick-Start-Guide)

## Troubleshooting

Please submit an issue at: [Issues](https://github.com/Nexenta/edgefs-csi/issues)
