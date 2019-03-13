package edgefs

import (
	"fmt"
	"testing"
)

func Test_EdgefsIscsiVolumeParse(t *testing.T) {

	edgefsIscsiVol1 := "1@cltest/test/bk1/lun1"
	vol1, err := ParseEdgefsIscsiVolumeStr(edgefsIscsiVol1)

	if err != nil {
		t.Errorf("%s", err)
		return
	}

	edgefsIscsiVol2 := "2@cltest/test/bk1/lun2"
	vol2, err := ParseEdgefsIscsiVolumeStr(edgefsIscsiVol2)
	if err != nil {
		fmt.Printf("Error %s", err)
	}

	edgefsIscsiVol3 := "3@cltest/test/bk1/lun3"
	vol3, err := ParseEdgefsIscsiVolumeStr(edgefsIscsiVol3)
	if err != nil {
		fmt.Printf("Error %s", err)
	}

	t.Log(fmt.Sprintf("%+v", vol1))
	t.Log(fmt.Sprintf("%+v", vol2))
	t.Log(fmt.Sprintf("%+v", vol3))

}

func Test_EdgefsNfsVolumeParse(t *testing.T) {

	edgefsNfsVol1 := "2,test/nfs-test@cltest/test/nfs-test1"
	vol1, err := ParseEdgefsNfsVolumeStr(edgefsNfsVol1)

	if err != nil {
		t.Errorf("%s", err)
		return
	}

	edgefsNfsVol2 := "3,test/nfs-test@cltest/test/nfs-test2"
	vol2, err := ParseEdgefsNfsVolumeStr(edgefsNfsVol2)
	if err != nil {
		fmt.Printf("Error %s", err)
	}

	edgefsNfsVol3 := "5,test/nfs-test@cltest/test/nfs-test5"
	vol3, err := ParseEdgefsNfsVolumeStr(edgefsNfsVol3)
	if err != nil {
		fmt.Printf("Error %s", err)
	}

	t.Log(fmt.Sprintf("%+v", vol1))
	t.Log(fmt.Sprintf("%+v", vol2))
	t.Log(fmt.Sprintf("%+v", vol3))

}

func CreateServiceData(svc IEdgefsService, rawVolumes []string) (ServiceData, error) {

	volumes := make([]IEdgefsVolume, 0)
	for _, volume := range rawVolumes {

		var newVolume IEdgefsVolume
		var err error
		switch t := svc.(type) {
		case *EdgefsNfsService:
			newVolume, err = ParseEdgefsNfsVolumeStr(volume)
		case *EdgefsIscsiService:
			newVolume, err = ParseEdgefsIscsiVolumeStr(volume)
		default:
			return ServiceData{}, fmt.Errorf("Unknown EdgefsService type %s ", t)
		}

		if err != nil {
			return ServiceData{}, err
		}
		volumes = append(volumes, newVolume)
	}

	return ServiceData{Service: svc, Volumes: volumes}, nil
}

func GetNfsClusterData(config map[string]string) (ClusterData, error) {
	/* Nfs raw volumes */
	nfs01Svc := &EdgefsNfsService{EdgefsService: EdgefsService{Name: "nfs01", Type: EdgefsServiceType_NFS, Status: "active", Entrypoint: "20.20.20.01:2049"}}
	nfs01SvcData := []string{"1,test/nfs-test1@cltest/test/nfs-test1", "2,test/nfs-test2@cltest/test/nfs-test2"}
	nfs02Svc := &EdgefsNfsService{EdgefsService: EdgefsService{Name: "nfs02", Type: EdgefsServiceType_NFS, Status: "active", Entrypoint: "20.20.20.02:2049"}}
	nfs02SvcData := []string{"1,nfs-test1@cltest/test/nfs-test1", "3,test/nfs-new-test@cltest/test/nfs-new-test"}

	nfs01ServiceData, err := CreateServiceData(nfs01Svc, nfs01SvcData)
	if err != nil {
		return ClusterData{}, err
	}
	nfs02ServiceData, err := CreateServiceData(nfs02Svc, nfs02SvcData)
	if err != nil {
		return ClusterData{}, err
	}

	return ClusterData{ServicesData: []ServiceData{nfs01ServiceData, nfs02ServiceData}}, nil
}

func Test_NfsClusterData(t *testing.T) {

	clusterConfigOptions := map[string]string{}
	clusterConfigOptions["cluster"] = "cltest"
	clusterConfigOptions["tenant"] = "test"

	volumeHandle := "nfs01@cltest/test/nfs-test1"
	t.Log(fmt.Sprintf("Nfs VolumeHandle : %+v", volumeHandle))

	volumeId, err := ParseNfsVolumeID(volumeHandle, clusterConfigOptions)
	if err != nil {
		t.Errorf("Error: %s", err)
		return
	}

	clusterData, err := GetNfsClusterData(clusterConfigOptions)
	if err != nil {
		t.Errorf("Error: %s", err)
		return
	}
	t.Log(fmt.Sprintf("ClusterData: %+v", clusterData))

	serviceData, err := clusterData.FindServiceDataByVolumeID(volumeId)
	if err != nil {
		t.Errorf("Error: %s", err)
		return
	}

	t.Log(fmt.Sprintf("serviceData: %+v", serviceData))

	mountParams, err := serviceData.GetVolumeMountParams(volumeId)
	if err != nil {
		t.Errorf("Error: %s", err)
		return
	}

	t.Log(fmt.Sprintf("Volume: %+v mount options %+v", volumeId, mountParams))
}

func GetIscsiClusterData(config map[string]string) (ClusterData, error) {

	/* Iscsi raw volumes */
	iscsi01Svc := &EdgefsIscsiService{EdgefsService: EdgefsService{Name: "iscsi01", Type: EdgefsServiceType_ISCSI, Status: "active", Entrypoint: "20.20.20.101:3260"}, TargetName: "iqn.2018-11.edgefs.io", TargetID: "36573"}
	iscsi01SvcData := []string{"1@cltest/test/bk1/lun1"} //, "2@cltest/test/bk1/lun2", "3@cltest/test/bk1/lun3"}
	iscsi02Svc := &EdgefsIscsiService{EdgefsService: EdgefsService{Name: "iscsi02", Type: EdgefsServiceType_ISCSI, Status: "active", Entrypoint: "20.20.20.102:3260"}, TargetName: "iqn.2018-11.edgefs.io", TargetID: "36574"}
	iscsi02SvcData := []string{"1@cltest/test/bk1/lun1", "2@cltest/test/bk1/new-lun"}

	iscsi01ServiceData, err := CreateServiceData(iscsi01Svc, iscsi01SvcData)
	if err != nil {
		return ClusterData{}, err
	}
	iscsi02ServiceData, err := CreateServiceData(iscsi02Svc, iscsi02SvcData)
	if err != nil {
		return ClusterData{}, err
	}

	return ClusterData{ServicesData: []ServiceData{iscsi01ServiceData, iscsi02ServiceData}}, nil
}

func Test_IscsiClusterData(t *testing.T) {

	clusterConfigOptions := map[string]string{}
	clusterConfigOptions["cluster"] = "cltest"
	clusterConfigOptions["tenant"] = "test"
	clusterConfigOptions["bucket"] = "bk1"

	volumeHandle := "iscsi02@new-lun"
	t.Log(fmt.Sprintf("Iscsi VolumeHandle : %+v", volumeHandle))

	volumeId, err := ParseIscsiVolumeID(volumeHandle, clusterConfigOptions)
	if err != nil {
		t.Errorf("Error: %s", err)
		return
	}

	clusterData, err := GetIscsiClusterData(clusterConfigOptions)
	if err != nil {
		t.Errorf("Error: %s", err)
		return
	}

	t.Log(fmt.Sprintf("ClusterData: %+v", clusterData))
	serviceData, err := clusterData.FindServiceDataByVolumeID(volumeId)
	if err != nil {
		t.Errorf("Error: %s", err)
		return
	}

	mountParams, err := serviceData.GetVolumeMountParams(volumeId)
	if err != nil {
		t.Errorf("Error: %s", err)
		return
	}

	t.Log(fmt.Sprintf("Volume: %+v mount options %+v", volumeId, mountParams))
}

func Test_CreateIscsiVolume(t *testing.T) {

	clusterConfigOptions := map[string]string{}
	clusterConfigOptions["cluster"] = "cltest"
	clusterConfigOptions["tenant"] = "test"
	clusterConfigOptions["bucket"] = "bk1"

	volumeHandle := "new-lun334"
	t.Log(fmt.Sprintf("Iscsi VolumeHandle : %+v", volumeHandle))

	volumeId, err := ParseIscsiVolumeID(volumeHandle, clusterConfigOptions)
	if err != nil {
		t.Errorf("Error: %s", err)
		return
	}

	clusterData, err := GetIscsiClusterData(clusterConfigOptions)
	if err != nil {
		t.Errorf("Error: %s", err)
		return
	}

	t.Log(fmt.Sprintf("ClusterData: %+v", clusterData))
	serviceData, err := clusterData.FindServiceDataByVolumeID(volumeId)
	t.Log(fmt.Sprintf("ServiceData : %+v", serviceData))

	// Service specified in VolumeID doesn't exists
	if IsEdgefsNoServiceAvailableError(err) {
		t.Errorf("Error: %s", err)
		return
	}

	// Service specified in VolumeID doesn't exists
	if IsEdgefsServiceNotExistsError(err) {
		t.Errorf("Error: %s", err)
		return
	}

	// Volume not exists in cluster defined by filters
	if IsEdgefsVolumeNotExistsError(err) {

		t.Log(fmt.Sprintf("Volume %s not exists", volumeId.GetObjectPath()))
		appServiceData, err := clusterData.FindApropriateServiceData("randomservicepolicy")
		if err == nil {
			t.Log(fmt.Sprintf("Appropriate service is %+v", appServiceData))
		}
	}
}

/*
	mountParams, err := serviceData.GetVolumeMountParams(volumeId)
	if err != nil {
		t.Errorf("Error: %s", err)
		return
	}

	t.Log(fmt.Sprintf("Volume: %+v mount options %+v", volumeId, mountParams))
*/
