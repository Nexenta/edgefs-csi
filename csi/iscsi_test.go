package csi

import (
	"./drivers/iscsi"
	"fmt"
	"testing"
)

const (
	volumeId   = "cltest/test/bk1/lun2"
	targetpath = "/mnt/lun2"
	tp         = "10.99.15.58:3260"
	iqn        = "iqn.2018-11.edgefs.io:31393"
	lun        = 2
)

func TestAttachISCSIVolume(t *testing.T) {

	vpi := iscsi.VolumePublishInfo{
		FilesystemType: "ext4",
		VolumeAccessInfo: iscsi.VolumeAccessInfo{
			IscsiAccessInfo: iscsi.IscsiAccessInfo{
				IscsiTargetPortal: tp,
				IscsiTargetIQN:    iqn,
				IscsiPortals:      []string{},
				IscsiLunNumber:    int32(lun),
			},
		},
	}

	err := iscsi.AttachISCSIVolume(volumeId, targetpath, &vpi)
	if err != nil {
		fmt.Printf("AttachVolume error: %s\n", err)
		return
	}

	scsiDeviceInfo, err := iscsi.GetDeviceInfoForMountPath(targetpath)
	if err != nil {
		fmt.Printf("GetDeviceInfoForMountPath error: %s\n", err)
		return
	}

	fmt.Printf("scsiDeviceInfo %+v\n", scsiDeviceInfo)
	fmt.Printf("Done")
}

func TestDetachISCSIVolume(t *testing.T) {
	scsiDeviceInfo, err := iscsi.GetDeviceInfoForMountPath(targetpath)
	if err != nil {
		fmt.Printf("GetDeviceInfoForMountPath error: %s\n", err)
		return
	}

	fmt.Printf("scsiDeviceInfo %+v\n", scsiDeviceInfo)

	err = iscsi.PrepareDeviceAtMountPathForRemoval(targetpath, true)
	if err != nil {
                fmt.Printf("PrepareDeviceAtMountPathForRemoval error: %s\n", err)
                return
        }

	fmt.Printf("Detach done")
}

func TestGetISCSIDevices(t *testing.T) {
	dev, refCount, err := iscsi.GetDeviceNameFromMount(targetpath)
	if err != nil {
                fmt.Printf("GetDeviceNameFromMount error: %s\n", err)
                return
        }
	fmt.Printf("GetDeviceNameFromMount device: %s, %d\n", dev, refCount)

	devices, err := iscsi.GetMountedISCSIDevices()
	if err != nil {
                fmt.Printf("GetMountedISCSIDevices error: %s\n", err)
                return
        }

	fmt.Printf("Mounted devices:\n")
	for _, devInfo := range devices {
		 fmt.Printf("%+v\n", *devInfo)
	}

}
