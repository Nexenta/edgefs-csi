package csi

import (
	"fmt"
)

// Role - role of this instance of driver
type Role string

const (
	// RoleAll - run driver as both controller and node instance
	RoleAll Role = "all"

	// RoleController - run driver as a controller instance (Identity server + Controller server)
	// This role should be used with:
	// 	- "csi-provisioner" sidecar container that watches Kubernetes PersistentVolumeClaim objects
	//		and triggers CreateVolume/DeleteVolume against a CSI endpoint
	//	- "csi-attacher" sidecar container that watches Kubernetes VolumeAttachment objects
	// 		and triggers ControllerPublish/Unpublish against a CSI endpoint
	RoleController Role = "controller"

	// RoleNode - to run driver as a node instance (Identity server + Node server), runs on each K8s node
	// This role should be used with "driver-registrar", sidecar container that:
	//	1) registers the CSI driver with kubelet
	//	2) adds the drivers custom NodeId to a label on the Kubernetes Node API Object
	RoleNode Role = "node"
)

func (r Role) String() string {
	return string(r)
}

// IsController - is this a controller role
func (r Role) IsController() bool {
	return r == RoleController || r == RoleAll
}

// IsNode - is this a node role
func (r Role) IsNode() bool {
	return r == RoleNode || r == RoleAll
}

// Roles - list of the driver roles
var Roles = []Role{RoleAll, RoleController, RoleNode}

// ParseRole - create role from user input
func ParseRole(from string) (Role, error) {
	for _, r := range Roles {
		if string(r) == from {
			return r, nil
		}
	}
	return RoleAll, fmt.Errorf(
		"role '%s' is not one of supported roles: %v, default '%s' will be used",
		from,
		Roles,
		RoleAll,
	)
}

