/*
 * Copyright (c) 2015-2018 Nexenta Systems, Inc.
 *
 * This file is part of EdgeFS Project
 * (see https://github.com/Nexenta/edgefs).
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package edgefs

import (
	"fmt"
	"time"
	"strconv"
	"../errors"

	logrus "github.com/sirupsen/logrus"
)

const (
	defaultUsername            string = "admin"
	defaultPassword            string = "admin"
	defaultNFSMountOptions     string = "vers=3,tcp"
	defaultK8sEdgefsNamespace  string = "edgefs"
	defaultK8sEdgefsMgmtPrefix string = "edgefs-mgmt"
	defaultK8sClientInCluster  bool   = true
	defaultFsType		   string = "ext4"
	defaultServiceBalancerPolicy    string = "minexportspolicy"
        defaultChunkSize           int32    = 16384
        defaultBlockSize           int32    = 4096
)

/*IEdgeFS interface to provide base methods for CSI driver client methods */
type IEdgeFS interface {
	/* CSI NFS client methods */
	CreateNfsVolume(csiVolumeID string, size int, options map[string]string) (string, error)
	DeleteNfsVolume(csiVolumeID string) error

	/* CSI ISCSI client methods */
	CreateIscsiVolume(name ,sourceSnapshot string, size int64, options map[string]string) (string, error)
        DeleteIscsiVolume(csiVolumeID string) error

	/* Snapshots */
	CreateObjectSnapshot(csiVolumeID, snapName string) (SnapshotInfo, error)
	DeleteObjectSnapshot(csiSnapshotID string) (error)
	ListObjectSnapshots(csiVolumeID, pattern string) ([]SnapshotInfo, error)

	/* returns all available cluster volumes for current driver type */
	ListVolumes() ([]IEdgefsVolume, error)
	ListServices(serviceName ...string) ([]IEdgefsService, error)

	IsClusterExists(clusterName string) bool
	IsTenantExists(clusterName, tenantName string) bool
	IsBucketExists(clusterName, tenantName, bucketName string) bool
	GetClusterData(serviceName ...string) (ClusterData, error)

	PrepareConfigMap() map[string]string
	GetClusterConfig() (config *EdgefsClusterConfig)
}

type EdgeFS struct {
	provider            IEdgeFSProvider
	clusterConfig       EdgefsClusterConfig
	backendType         string
	logger		    *logrus.Entry
}

func PrepareClusterConfigDefaultValues(config *EdgefsClusterConfig, backendType string) error {
	if config == nil {
		return fmt.Errorf("Pointer to ClusterConfig is null")
	}
	config.K8sClientInCluster = defaultK8sClientInCluster

	/* Apply default values here */
        if len(config.Username) == 0 {
                config.Username = defaultUsername
        }

        if len(config.Password) == 0 {
                config.Password = defaultPassword
        }

	if config.K8sEdgefsNamespace == "" {
                config.K8sEdgefsNamespace = defaultK8sEdgefsNamespace
        }

        if config.K8sEdgefsMgmtPrefix == "" {
                config.K8sEdgefsMgmtPrefix = defaultK8sEdgefsMgmtPrefix
        }

        if backendType == EdgefsServiceType_NFS {
		if len(config.MountOptions) == 0 {
	              config.MountOptions = defaultNFSMountOptions
		}
        }

	if config.ChunkSize == 0  {
		config.ChunkSize = defaultChunkSize
	}

	if config.BlockSize == 0  {
                config.BlockSize = defaultBlockSize
        }

	if len(config.FsType) == 0 {
		config.FsType = defaultFsType
	}

	if len(config.ServiceBalancerPolicy) == 0 {
		config.ServiceBalancerPolicy = defaultServiceBalancerPolicy
	}

	return nil
}

/*InitEdgeFS reads config and discovers Edgefs clusters*/
func InitEdgeFS(backendType string, logger *logrus.Entry) (edgefs IEdgeFS, err error) {
	var config EdgefsClusterConfig
	var provider IEdgeFSProvider
	l := logger.WithField("cmp", "edgefs")

	config, err = ReadParseConfig()
	if err != nil {
		err = fmt.Errorf("failed to read config file.  Error: %+v", err)
		l.WithField("func", "InitEdgeFS()").Errorf("%+v", err)
		return nil, err
	}

	err = PrepareClusterConfigDefaultValues(&config, backendType)
	if err != nil {
                l.WithField("func", "InitEdgeFS()").Errorf("%+v", err)
                return nil, err
        }

	// No address information for k8s Edgefs cluster
	if config.EdgefsProxyAddr == "" {
		isClusterExists, _ := DetectEdgefsK8sCluster(&config)

		if !isClusterExists {
			return nil, fmt.Errorf("No EdgeFS Cluster has been found")
		}
	}

	//default port
	clusterPort := int16(6789)
	i, err := strconv.ParseInt(config.EdgefsProxyPort, 10, 16)
	if err == nil {
		clusterPort = int16(i)
	}

	provider = InitEdgeFSProvider(config.EdgefsProxyAddr, clusterPort, config.Username, config.Password, logger)
	err = provider.CheckHealth()
	if err != nil {
		l.WithField("func", "InitEdgeFS()").Errorf("InitEdgeFS failed during CheckHealth : %v", err)
		return nil, err
	}
	l.WithField("func", "InitEdgeFS()").Debugf("Check healtz for %s is OK!", config.EdgefsProxyAddr)

	EdgeFSInstance := &EdgeFS{
		provider:            provider,
		clusterConfig:       config,
		backendType:         backendType,
		logger:		     l,
	}

	return EdgeFSInstance, nil
}

func (edgefs *EdgeFS) GetClusterConfig() (config *EdgefsClusterConfig) {
	return &edgefs.clusterConfig
}

func (edgefs *EdgeFS) PrepareConfigMap() map[string]string {
	configMap := make(map[string]string)

	if edgefs.clusterConfig.Cluster != "" {
		configMap["cluster"] = edgefs.clusterConfig.Cluster
	}

	if edgefs.clusterConfig.Tenant != "" {
		configMap["tenant"] = edgefs.clusterConfig.Tenant
	}

	if edgefs.backendType == EdgefsServiceType_ISCSI {
		if edgefs.clusterConfig.Bucket != "" {
		        configMap["bucket"] = edgefs.clusterConfig.Bucket
	        }
	}

	return configMap
}

/*CreateVolume creates bucket and serve it via edgefs service*/
func (edgefs *EdgeFS) CreateNfsVolume(csiVolumeID string, size int, options map[string]string) (string, error) {

	l := edgefs.logger.WithField("func", "CreateNfsVolume()")
	configMap := edgefs.PrepareConfigMap()

	volumeID, err := ParseNfsVolumeID(csiVolumeID, configMap)
	if err != nil {
		return "", err
	}

	// check cluster existance in Edgefs cluster
        if !edgefs.IsClusterExists(volumeID.Cluster) {
                return "", fmt.Errorf("No cluster name %s found", volumeID.Cluster)
        }

        // check tenant existance in Edgefs cluster
        if !edgefs.IsTenantExists(volumeID.Cluster, volumeID.Tenant) {
                return "", fmt.Errorf("No cluster/tenant name %s/%s found", volumeID.Cluster, volumeID.Tenant)
        }


	// get all services information to find already existing volume by path
	clusterData, err := edgefs.GetClusterData()
	if err != nil {
		l.Errorf("Couldn't get ClusterData : %+v", err)
		return "", err
	}

	l.Infof("ClusterData: '%+v'", clusterData)
	//try to find pointer to Edgefs service by specified volumeID
	serviceData, err := clusterData.FindServiceDataByVolumeID(volumeID)

	if err != nil {
		// throws error on any error but IsEdgefsVolumeNotExistsError
		if !errors.IsEdgefsVolumeNotExistsError(err) {
			return "", err
		}
		// it's OK. Volume doesnt exists. Need to create new one
	} else {
		//Volume related to VolumeID already exist. Return the current one
		return volumeID.GetObjectPath(), nil
	}

	// ServiceName not defined for  current VolumeID 
	if len(volumeID.GetServiceName()) == 0 {

		// find apropriate service to serve
		serviceData, err = clusterData.FindApropriateServiceData(edgefs.GetClusterConfig().ServiceBalancerPolicy)
		l.Infof("Appropriate serviceData: '%+v'", serviceData)
		if err != nil {
			l.Errorf("Appropriate serviceData selection failed: %+v", err)
			return "", err
		}

		// assign appropriate service name to VolumeID
		volumeID.SetServiceName(serviceData.GetService().GetName())
	}

	// check tenant existance in Edgefs cluster
	if !edgefs.provider.IsBucketExist(volumeID.Cluster, volumeID.Tenant, volumeID.Bucket) {
		l.Debugf("Bucket %s doesnt exist. Creating new one", volumeID.GetObjectPath())
		err := edgefs.provider.CreateBucket(volumeID.Cluster, volumeID.Tenant, volumeID.Bucket, 0, options)
		if err != nil {
			l.Error(err)
			return "", err
		}
		l.Debugf("Bucket %s created", volumeID.GetObjectPath())
	} else {
		l.Debugf("Bucket %s already exists", volumeID.GetObjectPath())
	}

	// setup service configuration if asked
	if options["acl"] != "" {
		err := edgefs.provider.SetServiceAclConfiguration(volumeID.Service, volumeID.Tenant, volumeID.Bucket, options["acl"])
		if err != nil {
			l.Error(err)
		}
	}

	volumePath, err := edgefs.provider.ServeBucket(volumeID.Service,
							serviceData.GetService().GetK8SSvcName(),
							serviceData.GetService().GetK8SNamespace(),
							EdgefsNfsVolume{Cluster: volumeID.Cluster, Tenant: volumeID.Tenant, Bucket: volumeID.Bucket},
							VolumeSettings{})
	if err != nil {
		l.Error(err)
		return "", err
	}
	l.Infof("New volume: %s objectPath: %s served to service %s", volumeID.GetObjectPath(), volumePath, volumeID.Service)

	return volumeID.GetObjectPath(), nil
}

/*DeleteVolume remotely deletes bucket on edgefs service*/
func (edgefs *EdgeFS) DeleteNfsVolume(csiVolumeID string) (err error) {
	l := edgefs.logger.WithField("func", "DeleteNfsVolume()")

	configMap := edgefs.PrepareConfigMap()
	volumeID, err := ParseNfsVolumeID(csiVolumeID, configMap)

	 // check cluster existance in Edgefs cluster
        if !edgefs.IsClusterExists(volumeID.Cluster) {
                return fmt.Errorf("No cluster name %s found", volumeID.Cluster)
        }

        // check tenant existance in Edgefs cluster
        if !edgefs.IsTenantExists(volumeID.Cluster, volumeID.Tenant) {
                return fmt.Errorf("No cluster/tenant name %s/%s found", volumeID.Cluster, volumeID.Tenant)
        }

	clusterData, err := edgefs.GetClusterData()
        if err != nil {
                l.Errorf("Couldn't get ClusterData: %s", err)
                return err
        }

	// find pointer to Edgefs service by VolumeID 
	serviceData, err := clusterData.FindServiceDataByVolumeID(volumeID)

	if err != nil {
		l.Warnf("FindServiceDataByVolumeID: %s", err)
		// returns nil, because there is no service with such volume
		return nil
	}

	// apply service name to volumeID
        volumeID.SetServiceName(serviceData.GetService().GetName())

	l.Infof("VolumeID: '%+v'", volumeID)

	// before unserve bucket we need to unset ACL property
	edgefs.provider.SetServiceAclConfiguration(volumeID.Service, volumeID.Tenant, volumeID.Bucket, "")

	err = edgefs.provider.UnserveBucket(volumeID.Service, serviceData.GetService().GetK8SSvcName(), serviceData.GetService().GetK8SNamespace(),
		EdgefsNfsVolume{Cluster: volumeID.Cluster, Tenant:  volumeID.Tenant, Bucket: volumeID.Bucket})

	if err != nil {
		l.Infof("UnserveBucket failed with error %s", err)
	}

	if edgefs.provider.IsBucketExist(volumeID.Cluster, volumeID.Tenant, volumeID.Bucket) {
		edgefs.provider.DeleteBucket(volumeID.Cluster, volumeID.Tenant, volumeID.Bucket, edgefs.clusterConfig.ForceVolumeDeletion)
	}

	return nil
}

/*CreateVolume creates bucket and serve it via edgefs service*/
func (edgefs *EdgeFS) CreateIscsiVolume(name ,sourceSnapshot string, size int64, options map[string]string) (string, error) {
	l := edgefs.logger.WithField("func", "CreateIscsiVolume()")
	configMap := edgefs.PrepareConfigMap()
	l.Infof("csiVolumeName: %s, sourceSnapshot: %s", name, sourceSnapshot)
	volumeID, err := ParseIscsiVolumeID(name, configMap)
	if err != nil {
		return "", err
	}

	l.Infof("Parser VolumeID: '%+v'", volumeID)
	// check cluster existance in Edgefs cluster
	if !edgefs.IsClusterExists(volumeID.Cluster) {
		return "", fmt.Errorf("No cluster '%s' found", volumeID.Cluster)
	}

	// check tenant existance in Edgefs cluster
	if !edgefs.IsTenantExists(volumeID.Cluster, volumeID.Tenant) {
		return "", fmt.Errorf("No tenant '%s/%s' found", volumeID.Cluster, volumeID.Tenant)
	}

	// check bucket existance in Edgefs cluster
	if !edgefs.IsBucketExists(volumeID.Cluster, volumeID.Tenant, volumeID.Bucket) {
		return "", fmt.Errorf("No bucket '%s/%s/%s' found", volumeID.Cluster, volumeID.Tenant, volumeID.Bucket)
	}

	// get all services information to find already existing volume by path
	clusterData, err := edgefs.GetClusterData()
	if err != nil {
		l.Errorf("Couldn't get ClusterData : '%+v'", err)
		return "", err
	 }

	l.Infof("ClusterData: '%+v'", clusterData)
	//try to find pointer to Edgefs service by specified volumeID
	serviceData, err := clusterData.FindServiceDataByVolumeID(volumeID)

	if err != nil {
		// throws error on any error but IsEdgefsVolumeNotExistsError
		if !errors.IsEdgefsVolumeNotExistsError(err) {
			return "", err
		}
		// it's OK. Volume doesnt exists. Need to create new one
	} else {
		//Volume related to VolumeID already exist. Return the current one
		return volumeID.GetObjectPath(), nil
	}

	// ServiceName not defined for  current VolumeID
	if len(volumeID.GetServiceName()) == 0 {

		// find apropriate service to serve
		serviceData, err = clusterData.FindApropriateServiceData(edgefs.GetClusterConfig().ServiceBalancerPolicy)
		l.Infof("Appropriate serviceData: '%+v'", serviceData)
		if err != nil {
			l.Errorf("Appropriate service selection failed: '%+v'", err)
			return "", err
		}

		// assign appropriate service name to VolumeID
		volumeID.SetServiceName(serviceData.GetService().GetName())
	}

	//Clone from source snapshot if defined
	volumeIsCloned := false
	if sourceSnapshot != "" {
		snapshotID, err  := ParseIscsiSnapshotID(sourceSnapshot, configMap)
		if err != nil {
                        return "", fmt.Errorf("Couldn't parse snapshot ID: %s , Error: %s", sourceSnapshot, err)
                }

		cloneInfo ,err := edgefs.provider.CloneVolumeFromSnapshot(*volumeID, snapshotID)
		if err != nil {
			l.Error(err)
			return "", err
		}
		volumeIsCloned = true
		l.Infof("CloneInfo: '%+v'", cloneInfo)
	}

	/* Check volume settings */
	var chunkSize int32
	if chunkSizeStr, ok := options["chunksize"]; ok  {
		i, err := strconv.ParseInt(chunkSizeStr, 10, 32)
		if err == nil {
			result := int32(i)

			// power of two check
			if (result > 0 && ((result & (result-1)) == 0)) {
				chunkSize = result
			}
		}
	} else {
		chunkSize = edgefs.clusterConfig.ChunkSize
	}

	//TODO: Add blocksize map to check all cases
	var blockSize int32
	if blockSizeStr, ok := options["blocksize"]; ok  {
		i, err := strconv.ParseInt(blockSizeStr, 10, 32)
                if err == nil {
			blockSize = int32(i)
		}
	} else {
		blockSize = edgefs.clusterConfig.BlockSize
	}

	volumePath ,err := edgefs.provider.ServeObject(volumeID.Service,
						       serviceData.GetService().GetK8SSvcName(),
						       serviceData.GetService().GetK8SNamespace(),
						       EdgefsIscsiVolume{Cluster: volumeID.Cluster,
									 Tenant: volumeID.Tenant,
									 Bucket: volumeID.Bucket,
									 Object: volumeID.Object},
							VolumeSettings{
									IsClonedObject: volumeIsCloned,
									VolumeSize: size,
									ChunkSize: chunkSize,
								        BlockSize: blockSize,
									})
	if err != nil {
		l.Error(err)
		return "", err
	}

	l.Infof("%s, volumePath: %s served to service %s", volumeID.GetObjectPath(), volumePath,  volumeID.Service)

	return volumeID.GetObjectPath(), nil
}

/*DeleteIscsiVolume remotely deletes object on edgefs service*/
func (edgefs *EdgeFS) DeleteIscsiVolume(csiVolumeID string) (err error) {
	l := edgefs.logger.WithField("func", "DeleteIscsiVolume()")
	l.Debugf("csiVolumeID: '%s'", csiVolumeID)
       
        configMap := edgefs.PrepareConfigMap()
        volumeID, err := ParseIscsiVolumeID(csiVolumeID, configMap)

         // check cluster existance in Edgefs cluster
        if !edgefs.IsClusterExists(volumeID.Cluster) {
                return fmt.Errorf("No cluster %s found", volumeID.Cluster)
        }

        // check tenant existance in Edgefs cluster
        if !edgefs.IsTenantExists(volumeID.Cluster, volumeID.Tenant) {
                return fmt.Errorf("No tenant %s/%s found", volumeID.Cluster, volumeID.Tenant)
        }

	// check tenant existance in Edgefs cluster
        if !edgefs.IsBucketExists(volumeID.Cluster, volumeID.Tenant, volumeID.Bucket) {
                return fmt.Errorf("No bucket %s/%s/%s found", volumeID.Cluster, volumeID.Tenant, volumeID.Bucket)
        }

        clusterData, err := edgefs.GetClusterData()
        if err != nil {
                l.Errorf("Couldn't get ClusterData: '%s'", err)
                return err
        }

        // find pointer to Edgefs service by VolumeID
        serviceData, err := clusterData.FindServiceDataByVolumeID(volumeID)

        if err != nil {
                l.Warnf("FindServiceDataByVolumeID %s", err)
                // returns nil, because there is no service with such volume
                return nil
        }

        // apply service name to volumeID
        volumeID.SetServiceName(serviceData.GetService().GetName())

        l.Infof("VolumeID: '%+v'", volumeID)

        // before unserve bucket we need to unset ACL property
        edgefs.provider.SetServiceAclConfiguration(volumeID.Service, volumeID.Tenant, volumeID.Bucket, "")

	l.Infof("deleting object: '%s'", volumeID.GetObjectPath())
	volumeParams, err := serviceData.GetEdgefsVolumeParams(volumeID)
	if err != nil {
                l.Errorf("GetEdgefsVolumeParams Error: %s", err)
                // returns nil, because there is no service with such volume
                return nil
        }

	lunNumber, err := strconv.ParseUint(volumeParams["lunNumber"], 10, 32)
	if err != nil {
		 l.Errorf("failed to convert %s to int32", volumeParams["lunNumber"])
		return nil
	}

        edgefs.provider.UnserveObject(volumeID.Service, serviceData.GetService().GetK8SSvcName(), serviceData.GetService().GetK8SNamespace(),
                EdgefsIscsiVolume{Cluster: volumeID.Cluster, Tenant: volumeID.Tenant, Bucket: volumeID.Bucket, Object: volumeID.Object, LunNumber: uint32(lunNumber)})

        return nil
}

func (edgefs *EdgeFS) CreateObjectSnapshot(csiVolumeID, snapName string) (SnapshotInfo, error) {
	l := edgefs.logger.WithField("func", "CreateObjectSnapshot()")
        l.Debugf("csiVolumeID: %s, snapName: %s", csiVolumeID, snapName)

        configMap := edgefs.PrepareConfigMap()
        volumeID, err := ParseIscsiVolumeID(csiVolumeID, configMap)

         // check cluster existance in Edgefs cluster
	/*
        if !edgefs.IsClusterExists(volumeID.Cluster) {
                return SnapshotInfo{}, fmt.Errorf("No cluster %s found", volumeID.Cluster)
        }

        // check tenant existance in Edgefs cluster
        if !edgefs.IsTenantExists(volumeID.Cluster, volumeID.Tenant) {
                return SnapshotInfo{}, fmt.Errorf("No tenant %s/%s found", volumeID.Cluster, volumeID.Tenant)
        }

        // check tenant existance in Edgefs cluster
        if !edgefs.IsBucketExists(volumeID.Cluster, volumeID.Tenant, volumeID.Bucket) {
                return SnapshotInfo{}, fmt.Errorf("No bucket %s/%s/%s found", volumeID.Cluster, volumeID.Tenant, volumeID.Bucket)
        }
	*/

	//TODO: Add object existance check

	snapshotPath := fmt.Sprintf("%s@%s", volumeID.GetObjectPath(), snapName)
	snapshotID, err := ParseIscsiSnapshotID(snapshotPath, configMap)
	if err != nil {
		return SnapshotInfo{}, fmt.Errorf("Couldn't parse snapshot ID: %s, %", snapshotPath, err)
	}

	isAlreadyExists, err := edgefs.provider.IsSnapshotExists(snapshotID)
	if err != nil {
                return SnapshotInfo{}, fmt.Errorf("Couldn't check snapshot %s existance: %s", snapshotPath, err)
        }

	if isAlreadyExists {
		//TODO: Set current time for snapshot, need to figure out how to get timestamp from snapshot
		timestamp := time.Now().Unix()
		l.Infof("Snapshot %s already exists", snapshotPath)
		return SnapshotInfo{SnapshotPath: snapshotPath, SourceVolume: volumeID.GetObjectPath(), CreationTime: timestamp}, nil
	}

	snapInfo ,err := edgefs.provider.CreateSnapshot(snapshotID)

	if err != nil {
                l.Error(err)
                return SnapshotInfo{}, err
        }

        l.Infof("volume: %s snapshot: %+v", volumeID.GetObjectPath(), snapInfo)
        return snapInfo, nil
}

func (edgefs *EdgeFS) DeleteObjectSnapshot(csiSnapshotID string) (error) {
	l := edgefs.logger.WithField("func", "DeleteObjectSnapshot()")
        l.Debugf("csiSnapshotID: '%s'", csiSnapshotID)

        configMap := edgefs.PrepareConfigMap()
	snapshotID, err := ParseIscsiSnapshotID(csiSnapshotID, configMap)
        if err != nil {
                return fmt.Errorf("Couldn't parse snapshot ID: %s, %", csiSnapshotID, err)
        }

        err = edgefs.provider.DeleteSnapshot(snapshotID)
        if err != nil {
                l.Error(err)
                return err
        }

        return nil
}

func (edgefs *EdgeFS) ListObjectSnapshots(csiVolumeID, pattern string) ([]SnapshotInfo, error) {
	l := edgefs.logger.WithField("func", "ListObjectSnapshots()")
        l.Debugf("csiVolumeID: '%s'", csiVolumeID)

        configMap := edgefs.PrepareConfigMap()
        volumeID, err := ParseIscsiVolumeID(csiVolumeID, configMap)

         // check cluster existance in Edgefs cluster
        if !edgefs.IsClusterExists(volumeID.Cluster) {
                return nil, fmt.Errorf("No cluster %s found", volumeID.Cluster)
        }

        // check tenant existance in Edgefs cluster
        if !edgefs.IsTenantExists(volumeID.Cluster, volumeID.Tenant) {
                return nil, fmt.Errorf("No tenant %s/%s found", volumeID.Cluster, volumeID.Tenant)
        }

        // check tenant existance in Edgefs cluster
        if !edgefs.IsBucketExists(volumeID.Cluster, volumeID.Tenant, volumeID.Bucket) {
                return nil, fmt.Errorf("No bucket %s/%s/%s found", volumeID.Cluster, volumeID.Tenant, volumeID.Bucket)
        }

        // Add object existance check
	snapshots, err := edgefs.provider.ListSnapshots(*volumeID, pattern)
	if err != nil {
                l.Error(err)
                return nil, err
        }
	return snapshots, nil
}

func (edgefs *EdgeFS) GetK8sEdgefsService(serviceName string) (resultService IK8SEdgefsService, err error) {
	k8sServices, err := GetEdgefsK8sClusterServices(edgefs.backendType, edgefs.clusterConfig.K8sEdgefsNamespace, edgefs.clusterConfig.K8sClientInCluster)
	if err != nil {
		return resultService, err
	}

	for _, k8sService := range k8sServices {
		if k8sService.GetName() == serviceName {
			return k8sService, err
		}
	}

	return resultService, fmt.Errorf("No Kubernetes service %s found", serviceName)
}

func (edgefs *EdgeFS) ListServices(serviceName ...string) (resultServices []IEdgefsService, err error) {
	l := edgefs.logger.WithField("func", "ListServices()")
	/*Kubernetes Edgefs service information */
	var k8sService IK8SEdgefsService
	var k8sServices []IK8SEdgefsService

	/*Edgefs service information */
        var services []IEdgefsService

	if len(serviceName) > 0 {
		k8sService, err = edgefs.GetK8sEdgefsService(serviceName[0])
		k8sServices = append(k8sServices, k8sService)
	} else {
		k8sServices, err = GetEdgefsK8sClusterServices(edgefs.backendType, edgefs.clusterConfig.K8sEdgefsNamespace, edgefs.clusterConfig.K8sClientInCluster)
	}
	l.Infof("Kubernetes Service list %+v", k8sServices)

	if err != nil {
		return resultServices, err
	}

	// Transform Kubernetes service info to Edgefs service type */
	for _, k8sSvc := range k8sServices {
		edgefsSvc, err := edgefs.provider.GetService(edgefs.backendType, k8sSvc.GetName())
		if err != nil {
			l.Warnf("Can't get Edgefs service %s, type:%s, Error: %s", k8sSvc.GetName(), k8sSvc.GetType(), err)
			continue
		}

		if k8sSvc.GetName() != edgefsSvc.GetName() {
			l.Warnf("Kubernetes service name doesn't match Edgefs service name %s:%s", k8sSvc.GetName(), edgefsSvc.GetName())
			continue
		}

		if k8sSvc.GetType() != edgefsSvc.GetType() {
			l.Warnf("Kubernetes service %s type %s  doesn't match Edgefs service %s type %s", k8sSvc.GetName(), edgefsSvc.GetName(), k8sSvc.GetType(), edgefsSvc.GetType())
                        continue

		}

		edgefsSvc.SetEntrypoint(k8sSvc.GetClusterIP()) //, k8sSvc.GetPort()))
		edgefsSvc.SetK8SSvcName(k8sSvc.GetK8SSvcName())
		edgefsSvc.SetK8SNamespace(k8sSvc.GetK8SNamespace())

		services = append(services, edgefsSvc)
	}

	for _, service := range services {

		//if ServiceFilter not empty, skip every service not presented in list(map)
		serviceFilterMap := edgefs.clusterConfig.GetServiceFilterMap()
		if len(serviceFilterMap) > 0 {
			if _, ok := serviceFilterMap[service.GetName()]; !ok {
				continue
			}
		}

		if (service.GetType() == edgefs.backendType && len(service.GetEntrypoint()) > 0) {
			resultServices = append(resultServices, service)
		}
	}
	l.Infof("ServiceList: '%+v'", resultServices)
	return resultServices, nil
}

/*ListVolumes list all available volumes */
func (edgefs *EdgeFS) ListVolumes() ([]IEdgefsVolume, error) {
	l := edgefs.logger.WithField("func", "ListVolumes()")

	//already filtered services with serviceFilter, service type e.t.c.
	services, err := edgefs.ListServices()
	if err != nil {
		return nil, err
	}

	volumes := make([]IEdgefsVolume, 0)
	for _, service := range services {

		serviceVolumes, err := edgefs.provider.ListVolumes(edgefs.backendType, service.GetName())
		if err != nil {
			l.Warnf("edgefs.provider.ListVolumes failed due %s", err)
			continue
		}
		volumes = append(volumes, serviceVolumes...)
	}

	return volumes, nil
}

/*GetClusterData if serviceName specified we will get data from the one service only */
func (edgefs *EdgeFS) GetClusterData(serviceName ...string) (ClusterData, error) {
	l := edgefs.logger.WithField("func", "GetClusterData()")
	//var services []IEdgefsService
	services, err := edgefs.ListServices()
	if err != nil {
		l.Warningf("No services in service list. %v", err)
		return ClusterData{}, err
	}

	if len(serviceName) > 0 {
		serviceFound := false
		for _, service := range services {
			if service.GetName() == serviceName[0] {
				services = []IEdgefsService{service}
				serviceFound = true
				break
			}
		}
		if serviceFound != true {
			l.Errorf("No service %s found in EdgeFS cluster", serviceName[0])
			return ClusterData{}, fmt.Errorf("No service %s found in EdgeFS cluster", serviceName[0])
		}
	}

	servicesData := make([]ServiceData, 0)
	for _, service := range services {


		volumes, err := edgefs.provider.ListVolumes(edgefs.backendType, service.GetName())
		if err != nil {
			l.Errorf("Failed to get service %s volumes. Error: %s ", service.GetName())
			return ClusterData{}, err
		}
		serviceData := ServiceData{Service: service, Volumes: volumes}
		servicesData = append(servicesData, serviceData)
	}

	return ClusterData{ServicesData: servicesData}, nil
}

func (edgefs *EdgeFS) IsClusterExists(clusterName string) bool {
	clusters, err := edgefs.provider.ListClusters()
	if err != nil {
		return false
	}

	for _, cluster := range clusters {
		if cluster == clusterName {
			return true
		}
	}
	return false
}

func (edgefs *EdgeFS) IsTenantExists(clusterName, tenantName string) bool {
	tenants, err := edgefs.provider.ListTenants(clusterName)
	if err != nil {
		return false
	}

	for _, tenant := range tenants {
		if tenant == tenantName {
			return true
		}
	}
	return false
}

func (edgefs *EdgeFS) IsBucketExists(clusterName, tenantName, bucketName string) bool {
	buckets, err := edgefs.provider.ListBuckets(clusterName, tenantName)
	if err != nil {
		return false
	}

	for _,  bucket := range buckets {
		if bucket == bucketName {
			return true
		}
	}
	return false
}

