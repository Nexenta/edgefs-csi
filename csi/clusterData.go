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
	"math/rand"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

type NfsServiceData struct {
	Service    EdgefsService
	NfsVolumes []EdgefsNFSVolume
}

func (nfsServiceData *NfsServiceData) FindNFSVolumeByVolumeID(volumeID VolumeID) (resultNfsVolume EdgefsNFSVolume, err error) {

	for _, nfsVolume := range nfsServiceData.NfsVolumes {
		if nfsVolume.VolumeID.FullObjectPath() == volumeID.FullObjectPath() {
			return nfsVolume, nil
		}
	}
	return resultNfsVolume, fmt.Errorf("Can't find NFS volume by VolumeID : %+v", volumeID)
}

func (nfsServiceData *NfsServiceData) GetNFSVolumeAndEndpoint(volumeID VolumeID) (nfsVolume EdgefsNFSVolume, endpoint string, err error) {
	nfsVolume, err = nfsServiceData.FindNFSVolumeByVolumeID(volumeID)
	if err != nil {
		return nfsVolume, "", err
	}

	return nfsVolume, fmt.Sprintf("%s:%s", nfsServiceData.Service.Network[0], nfsVolume.Share), err
}

/*ClusterData represents all available(enabled, has networks e.t.c) services and its NFS volumes on cluster
or among the listed in serviceFilter (if serviceFilter option specified)
*/
type ClusterData struct {
	nfsServicesData []NfsServiceData
}

/* template method to implement different Nfs service balancing types */
type nfsServiceSelectorFunc func(clusterData *ClusterData) (*NfsServiceData, error)

/* selects service with minimal export count from whole cluster (if serviceFilter ommited) or from serviceFilter's services */
func minimalExportsServiceSelector(clusterData *ClusterData) (*NfsServiceData, error) {
	if len(clusterData.nfsServicesData) > 0 {
		minService := &clusterData.nfsServicesData[0]

		for _, data := range clusterData.nfsServicesData[1:] {
			currentValue := len(data.NfsVolumes)
			if len(minService.NfsVolumes) > currentValue {
				minService = &data
			}
		}

		return minService, nil
	}

	return nil, fmt.Errorf("No NFS Services available along edgefs cluster")
}

/* selects random service from whole cluster (if serviceFilter ommited) or from serviceFilter's services */
func randomServiceSelector(clusterData *ClusterData) (*NfsServiceData, error) {
	if len(clusterData.nfsServicesData) > 0 {
		rand.Seed(time.Now().UnixNano())
		randomIndex := rand.Intn(len(clusterData.nfsServicesData) - 1)
		return &clusterData.nfsServicesData[randomIndex], nil
	}

	return nil, fmt.Errorf("No NFS Services available along edgefs cluster")
}

func processServiceSelectionPolicy(serviceSelector nfsServiceSelectorFunc, clusterData *ClusterData) (*NfsServiceData, error) {
	return serviceSelector(clusterData)
}

/*FindApropriateService find service with minimal export count*/
func (clusterData *ClusterData) FindApropriateServiceData(nfsBalancingPolicy string) (*NfsServiceData, error) {
	var serviceSelector nfsServiceSelectorFunc
	switch strings.ToLower(nfsBalancingPolicy) {
	// minServicePolicy
	case "minexportspolicy":
		log.Debugf("BalancerPolicy::MinimalExportsServiceSelector selected")
		serviceSelector = minimalExportsServiceSelector
	// randomServicePolicy
	case "randomservicepolicy":
		log.Debugf("BalancerPolicy::RandomServiceSelector selected")
		serviceSelector = randomServiceSelector
	default:
		log.Debugf("BalancerPolicy::MinimalExportsServiceSelector selected")
		serviceSelector = minimalExportsServiceSelector
	}

	return processServiceSelectionPolicy(serviceSelector, clusterData)
}

func (clusterData *ClusterData) FindServiceDataByVolumeID(volumeID VolumeID) (result *NfsServiceData, err error) {
	//log.Debug("FindServiceDataByVolumeID ")

	for _, data := range clusterData.nfsServicesData {
		for _, nfsVolume := range data.NfsVolumes {
			if nfsVolume.Path == volumeID.FullObjectPath() {
				return &data, nil
			}
		}
	}

	return nil, fmt.Errorf("Can't find NFS service data by VolumeID %s", volumeID)
}

/*FillNfsVolumes Fills outer volumes hashmap, format {VolumeID: volume nfs endpoint} */
func (clusterData *ClusterData) FillNfsVolumes(vmap map[string]string, defaultCluster string) {

	for _, data := range clusterData.nfsServicesData {
		for _, nfsVolume := range data.NfsVolumes {

			var volumePath string
			if defaultCluster != "" && nfsVolume.VolumeID.Cluster == defaultCluster {
				volumePath = nfsVolume.VolumeID.MinimalObjectPath()
			} else {
				volumePath = nfsVolume.VolumeID.FullObjectPath()
			}
			vname := volumePath
			vmap[vname] = fmt.Sprintf("%s:%s", data.Service.Network[0], nfsVolume.Share)
		}
	}
}

/* FindNfsServiceData finds and returns pointer to NfsServiceData stored in ClusterData */
func (clusterData *ClusterData) FindNfsServiceData(serviceName string) (serviceData *NfsServiceData, err error) {
	for _, serviceData := range clusterData.nfsServicesData {
		if serviceData.Service.Name == serviceName {
			return &serviceData, nil
		}
	}

	return nil, fmt.Errorf("Can't find Service Data by name %s", serviceName)
}
