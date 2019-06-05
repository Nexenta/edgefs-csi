package edgefs_test

import (
	"fmt"
	"testing"
	"github.com/Nexenta/edgefs-csi/csi"
)

func ListVolumes(e csi.IEdgeFS) {
	volumes, err := e.ListVolumes()
        if err != nil {
                fmt.Printf("Failed to ListVolumes: %s\n", err)
                return
        }

        fmt.Printf("Volumes  : %+v\n", volumes)
}

func CreateVolume(e csi.IEdgeFS, volumeID string) {
        volID, err := e.CreateVolume(volumeID, 0, make(map[string]string))
        if err != nil {
                fmt.Printf("Failed to CreateVolume: %s\n", err)
                return
        }
	fmt.Printf("Created VolumeID  : %+v\n", volID)
}

func DeleteVolume(e csi.IEdgeFS, volumeID string) {
        err := e.DeleteVolume(volumeID)
        if err != nil {
                fmt.Printf("Failed to DeleteVolume: %s\n", err)
                return
        }
}


func TestVolumes(t *testing.T) {
	e, err := csi.InitEdgeFS("edgefs_test")
	if err != nil {
                fmt.Printf("Failed to InitEdgeFS: %s\n", err)
                return
        }

	fmt.Printf("e  : %+v\n", e)

	volumeID := "cltest/test/bk1"

	ListVolumes(e)
	CreateVolume(e, volumeID)
	ListVolumes(e)
	DeleteVolume(e, volumeID)
	ListVolumes(e)
}
