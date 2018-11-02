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
package csi

import (
	"fmt"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
)

const (
	defaultChunkSize       int    = 1048576
	defaultUsername        string = "admin"
	defaultPassword        string = "admin"
	defaultNFSMountOptions string = "vers=3,tcp"
	defaultK8sEdgefsNamespace string = "edgefs"
	defaultK8sEdgefsMgmtPrefix string = "edgefs-mgmt"
	defaultK8sEdgefsNfsPrefix string = "edgefs-svc-nfs"
	defaultK8sClientInCluster bool = true
)

/*IEdgeFS interface to provide base methods */
type IEdgeFS interface {
	CreateVolume(volumeName string, size int, options map[string]string) (string, error)
	DeleteVolume(volumeID string) error
	ListVolumes() ([]EdgefsNFSVolume, error)
	CheckNfsServiceExists(serviceName string) error
	IsClusterExists(clusterName string) bool
	IsTenantExists(clusterName string, tenantName string) bool
	GetClusterDataByVolumeID(volumeID string) (VolumeID, ClusterData, error)
	GetClusterConfig() (config *EdgefsClusterConfig)
}

type EdgeFS struct {
	provider            IEdgeFSProvider
	clusterConfig       EdgefsClusterConfig
	isStandAloneCluster bool
}

type EdgefsClusterConfig struct {
	Name                  string
	EdgefsProxyAddr       string
	EdgefsProxyPort       string
	K8sEdgefsNamespace    string
	K8sEdgefsMgmtPrefix   string
	K8sEdgefsNfsPrefix    string
	K8sClientInCluster    bool
	Username              string
	Password              string
	Cluster               string
	Tenant                string
	NfsMountOptions       string `json:"nfsMountOptions"`
	ForceBucketDeletion   bool   `json:"forceBucketDeletion"`
	ServiceFilter         string `json:"serviceFilter"`
	ServiceBalancerPolicy string `json:"serviceBalancerPolicy"`
}

/* GetMountOptions */
func (config *EdgefsClusterConfig) GetMountOptions() (options []string) {

	mountOptionsParts := strings.Split(config.NfsMountOptions, ",")
	for _, option := range mountOptionsParts {
		options = append(options, strings.TrimSpace(option))
	}
	return options
}

func (config *EdgefsClusterConfig) GetServiceFilterMap() (filterMap map[string]bool) {

	if config.ServiceFilter != "" {
		filterMap = make(map[string]bool)
		services := strings.Split(config.ServiceFilter, ",")
		for _, srvName := range services {
			filterMap[strings.TrimSpace(srvName)] = true
		}
	}

	return filterMap
}

/*InitEdgeFS reads config and discovers Edgefs clusters*/
func InitEdgeFS(invoker string) (edgefs IEdgeFS, err error) {
	var config EdgefsClusterConfig
	var provider IEdgeFSProvider
	isStandAloneCluster := true

	config, err = ReadParseConfig()
	if err != nil {
		err = fmt.Errorf("failed to read config file %s Error: %s", edgefsConfigFile, err)
		log.Infof("%+v", err)
		return nil, err
	}

	/* Apply default values here */
	if len(config.Username) == 0 {
		config.Username = defaultUsername
	}

	if len(config.Password) == 0 {
		config.Password = defaultPassword
	}

	//set default NfsMountOptions values
	if len(config.NfsMountOptions) == 0 {
		config.NfsMountOptions = defaultNFSMountOptions
	}

	//
	// Kubernetes config
	//
	if config.K8sEdgefsNamespace == "" {
		config.K8sEdgefsNamespace = defaultK8sEdgefsNamespace
	}

	if config.K8sEdgefsMgmtPrefix == "" {
		config.K8sEdgefsMgmtPrefix = defaultK8sEdgefsMgmtPrefix
	}

	if config.K8sEdgefsNfsPrefix == "" {
		config.K8sEdgefsNfsPrefix = defaultK8sEdgefsNfsPrefix
	}

	config.K8sClientInCluster = defaultK8sClientInCluster

	// No address information for k8s Edgefs cluster
	if config.EdgefsProxyAddr == "" {
		isClusterExists, _ := DetectEdgefsK8sCluster(&config)

		if isClusterExists {
			isStandAloneCluster = false
		} else {
			return nil, fmt.Errorf("No EdgeFS Cluster has been found")
		}
	}

	//default port
	clusterPort := int16(6789)
	i, err := strconv.ParseInt(config.EdgefsProxyPort, 10, 16)
	if err == nil {
		clusterPort = int16(i)
	}

	provider = InitEdgeFSProvider(config.EdgefsProxyAddr, clusterPort, config.Username, config.Password)
	err = provider.CheckHealth()
	if err != nil {
		log.Error("InitEdgeFS failed during CheckHealth : %v", err)
		return nil, err
	}
	log.Debugf("Check healtz for %s is OK!", config.EdgefsProxyAddr)

	EdgeFSInstance := &EdgeFS{
		provider:            provider,
		clusterConfig:       config,
		isStandAloneCluster: isStandAloneCluster,
	}

	return EdgeFSInstance, nil
}

func (edgefs *EdgeFS) GetClusterConfig() (config *EdgefsClusterConfig) {
	return &edgefs.clusterConfig
}

func (edgefs *EdgeFS) CheckNfsServiceExists(serviceName string) error {
	edgefsService, err := edgefs.provider.GetService(serviceName)
	if err != nil {
		return fmt.Errorf("No EdgeFS service %s has been found", serviceName)
	}

	if edgefsService.ServiceType != "NFS" {
		return fmt.Errorf("Service %s is not nfs type service", edgefsService.Name)
	}

	// in case of In-Cluster edgefs configuration, there is no network configured
	if edgefs.isStandAloneCluster && len(edgefsService.Network) < 1 {
		return fmt.Errorf("Service %s isn't configured, no client network assigned", edgefsService.Name)
	}

	return nil
}

func (edgefs *EdgeFS) PrepareConfigMap() map[string]string {
	configMap := make(map[string]string)

	if edgefs.clusterConfig.Cluster != "" {
		configMap["cluster"] = edgefs.clusterConfig.Cluster
	}

	if edgefs.clusterConfig.Tenant != "" {
		configMap["tenant"] = edgefs.clusterConfig.Tenant
	}

	return configMap
}

// Checks only service name is missing in volume id
func IsNoServiceSpecified(missedParts map[string]bool) bool {
	if len(missedParts) == 1 {
		if _, ok := missedParts["service"]; ok {
			return true
		}
	}
	return false
}

/*CreateVolume creates bucket and serve it via edgefs service*/
func (edgefs *EdgeFS) CreateVolume(name string, size int, options map[string]string) (volumeID string, err error) {
	// get first service from list, should be changed later

	configMap := edgefs.PrepareConfigMap()
	volID, missedPathParts, err := ParseVolumeID(name, configMap)

	// throws error when can't substitute volume fill path, no service isn't error
	if err != nil && !IsNoServiceSpecified(missedPathParts) {
		log.Errorf("ParseVolumeID error : %+v", err)
		return "", err
	}

	// get all services information to find already existing volume by path
	clusterData, err := edgefs.GetClusterData()
	if err != nil {
		log.Errorf("Couldn't get ClusterData : %+v", err)
		return "", err
	}

	//try to find already existing service with specified volumeID
	serviceData, _ := clusterData.FindServiceDataByVolumeID(volID)
	if serviceData != nil {
		log.Warningf("Volume %s already exists via %s service", volID.FullObjectPath(), serviceData.Service.Name)
		// returns no error because volume already exists
		return volID.FullObjectPath(), nil
	}

	// When service name is missed in path notation, we should select appropriate service for new volume
	if IsNoServiceSpecified(missedPathParts) {

		// find apropriate service to serve
		appropriateServiceData, err := clusterData.FindApropriateServiceData(edgefs.GetClusterConfig().ServiceBalancerPolicy)

		if err != nil {
			log.Errorf("Appropriate service selection failed : %+v", err)
			return "", err
		}

		// assign appropriate service name to VolumeID
		volID.Service = appropriateServiceData.Service.Name
	}

	log.Infof("EdgeFS::CreateVolume Appropriate VolumeID : %+v", volID)
	serviceData, err = clusterData.FindNfsServiceData(volID.Service)
	//err = edgefs.CheckNfsServiceExists(volID.Service)
	if serviceData == nil {
		log.Error(err.Error)
		return "", err
	}

	// check for cluster name existance
	if !edgefs.IsClusterExists(volID.Cluster) {
		return "", fmt.Errorf("No cluster name %s found", volID.Cluster)
	}

	// check for tenant name existance
	if !edgefs.IsTenantExists(volID.Cluster, volID.Tenant) {
		return "", fmt.Errorf("No cluster/tenant name %s/%s found", volID.Cluster, volID.Tenant)
	}

	if !edgefs.provider.IsBucketExist(volID.Cluster, volID.Tenant, volID.Bucket) {
		log.Debugf("EdgeFS::CreateVolume Bucket %s/%s/%s doesnt exist. Creating one", volID.Cluster, volID.Tenant, volID.Bucket)
		err := edgefs.provider.CreateBucket(volID.Cluster, volID.Tenant, volID.Bucket, 0, options)
		if err != nil {
			log.Error(err)
			return "", err
		}
		log.Debugf("EdgeFS::CreateVolume Bucket %s/%s/%s created", volID.Cluster, volID.Tenant, volID.Bucket)
	} else {
		log.Debugf("EdgeFS::CreateVolume Bucket %s/%s/%s already exists", volID.Cluster, volID.Tenant, volID.Bucket)
	}

	// setup service configuration if asked
	if options["acl"] != "" {
		err := edgefs.provider.SetServiceAclConfiguration(volID.Service, volID.Tenant, volID.Bucket, options["acl"])
		if err != nil {
			log.Error(err)
		}
	}

	err = edgefs.provider.ServeBucket(volID.Service, serviceData.Service.K8SSvcName, serviceData.Service.K8SNamespace,
		volID.Cluster, volID.Tenant, volID.Bucket)
	if err != nil {
		log.Error(err)
		return "", err
	}
	log.Infof("EdgeFS::CreateVolume Bucket %s/%s/%s served to service %s", volID.Cluster, volID.Tenant, volID.Bucket, volID.Service)

	return volID.FullObjectPath(), nil
}

/*DeleteVolume remotely deletes bucket on edgefs service*/
func (edgefs *EdgeFS) DeleteVolume(volumeID string) (err error) {
	log.Debugf("EdgeFSProvider::DeleteVolume  VolumeID: %s", volumeID)

	var clusterData ClusterData
	configMap := edgefs.PrepareConfigMap()
	volID, missedPathParts, err := ParseVolumeID(volumeID, configMap)
	if err != nil {
		// Only service missed in path notation, we should select appropriate service for new volume
		if IsNoServiceSpecified(missedPathParts) {
			// get all services information to find service by path
			clusterData, err = edgefs.GetClusterData()
			if err != nil {
				return err
			}
		}
	} else {
		clusterData, err = edgefs.GetClusterData(volID.Service)
		if err != nil {
			return err
		}
	}

	// find service to serve
	serviceData, err := clusterData.FindServiceDataByVolumeID(volID)

	if err != nil {
		log.Warnf("Can't find service by volumeID %+v", volID)
		// returns nil, because there is no service with such volume
		return nil
	}

	// find nfs volume in service information
	nfsVolume, err := serviceData.FindNFSVolumeByVolumeID(volID)
	if err != nil {
		log.Warnf("Can't find served volume by volumeID %+v, Error: %s", volID, err)
		// returns nil, because volume already unserved
		return nil
	}
	log.Infof("EdgeFS::DeleteVolume by VolumeID: %+v", nfsVolume.VolumeID)

	// before unserve bucket we need to unset ACL property
	edgefs.provider.SetServiceAclConfiguration(nfsVolume.VolumeID.Service, nfsVolume.VolumeID.Tenant, nfsVolume.VolumeID.Bucket, "")

	edgefs.provider.UnserveBucket(nfsVolume.VolumeID.Service, serviceData.Service.K8SSvcName, serviceData.Service.K8SNamespace,
		nfsVolume.VolumeID.Cluster, nfsVolume.VolumeID.Tenant, nfsVolume.VolumeID.Bucket)

	if edgefs.provider.IsBucketExist(nfsVolume.VolumeID.Cluster, nfsVolume.VolumeID.Tenant, nfsVolume.VolumeID.Bucket) {
		edgefs.provider.DeleteBucket(nfsVolume.VolumeID.Cluster, nfsVolume.VolumeID.Tenant, nfsVolume.VolumeID.Bucket, edgefs.clusterConfig.ForceBucketDeletion)
	}

	return nil
}

func (edgefs *EdgeFS) GetK8sEdgefsService(serviceName string) (resultService EdgefsService, err error) {
	services, err := GetEdgefsK8sClusterServices(edgefs.clusterConfig.K8sClientInCluster,
	    edgefs.clusterConfig.K8sEdgefsNamespace)
	if err != nil {
		return resultService, err
	}

	for _, service := range services {
		if service.Name == serviceName {
			return service, err
		}
	}

	return resultService, fmt.Errorf("No service %s found", serviceName)
}

func (edgefs *EdgeFS) ListServices(serviceName ...string) (resultServices []EdgefsService, err error) {
	var service EdgefsService
	var services []EdgefsService
	if edgefs.isStandAloneCluster == true {
		if len(serviceName) > 0 {
			service, err = edgefs.provider.GetService(serviceName[0])
			services = append(services, service)
		} else {
			services, err = edgefs.provider.ListServices()
		}
	} else {
		if len(serviceName) > 0 {
			service, err = edgefs.GetK8sEdgefsService(serviceName[0])
			services = append(services, service)
		} else {
			services, err = GetEdgefsK8sClusterServices(edgefs.clusterConfig.K8sClientInCluster,
			    edgefs.clusterConfig.K8sEdgefsNamespace)
		}
		//log.Infof("Service list %+v", services)
	}

	if err != nil {
		return resultServices, err
	}

	for _, service := range services {

		//if ServiceFilter not empty, skip every service not presented in list(map)
		serviceFilterMap := edgefs.clusterConfig.GetServiceFilterMap()
		if len(serviceFilterMap) > 0 {
			if _, ok := serviceFilterMap[service.Name]; !ok {
				continue
			}
		}

		if (service.ServiceType == "NFS" && edgefs.isStandAloneCluster) ||
		   (service.ServiceType == "NFS" && len(service.Network) > 0) {
			resultServices = append(resultServices, service)
		}
	}
	return resultServices, nil
}

/*ListVolumes list all available volumes */
func (edgefs *EdgeFS) ListVolumes() (volumes []EdgefsNFSVolume, err error) {
	log.Debug("EdgeFSProvider::ListVolumes")

	//already filtered services with serviceFilter, service type e.t.c.
	services, err := edgefs.ListServices()
	if err != nil {
		return nil, err
	}

	for _, service := range services {

		nfsVolumes, err := edgefs.provider.ListNFSVolumes(service.Name)
		if err == nil {
			volumes = append(volumes, nfsVolumes...)
		}
	}

	return volumes, nil
}

/* returns ClusterData by raw volumeID string */
func (edgefs *EdgeFS) GetClusterDataByVolumeID(volumeID string) (VolumeID, ClusterData, error) {
	var clusterData ClusterData
	//log.Infof("GetClusterDataByVolumeID: %s", volumeID)
	configMap := edgefs.PrepareConfigMap()
	volID, missedPathParts, err := ParseVolumeID(volumeID, configMap)
	if err != nil {
		// Only service missed in path notation, we should select appropriate service for new volume
		if IsNoServiceSpecified(missedPathParts) {
			// get all services information to find service by path
			clusterData, err = edgefs.GetClusterData()
			if err != nil {
				return volID, clusterData, err
			}
		}
	} else {
		//log.Infof("GetClusterDataByVolumeID.GetClusterData: by service: %s", volID.Service)
		clusterData, err = edgefs.GetClusterData(volID.Service)
		if err != nil {
			return volID, clusterData, err
		}
	}

	return volID, clusterData, err
}

/*GetClusterData if serviceName specified we will get data from the one service only */
func (edgefs *EdgeFS) GetClusterData(serviceName ...string) (ClusterData, error) {

	clusterData := ClusterData{nfsServicesData: []NfsServiceData{}}
	var err error

	var services []EdgefsService

	services, err = edgefs.ListServices()
	if err != nil {
		log.Warningf("No services in service list. %v", err)
		return clusterData, err
	}

	if len(serviceName) > 0 {
		serviceFound := false
		for _, service := range services {
			if service.Name == serviceName[0] {
				services = []EdgefsService{service}
				serviceFound = true
				break
			}
		}
		if serviceFound != true {
			log.Errorf("No service %s found in EdgeFS cluster", serviceName[0])
			return clusterData, fmt.Errorf("No service %s found in EdgeFS cluster", serviceName[0])
		}
	}

	for _, service := range services {

		nfsVolumes, err := edgefs.provider.ListNFSVolumes(service.Name)
		if err == nil {
			nfsServiceData := NfsServiceData{Service: service, NfsVolumes: nfsVolumes}
			clusterData.nfsServicesData = append(clusterData.nfsServicesData, nfsServiceData)
		} else {
			log.Warningf("No nfs exports found for %s service. Error: %+v", service.Name, err)
		}
	}

	return clusterData, nil
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

func (edgefs *EdgeFS) IsTenantExists(clusterName string, tenantName string) bool {
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
