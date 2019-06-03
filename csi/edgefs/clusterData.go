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
	errors "github.com/Nexenta/edgefs-csi/csi/errors"

	"fmt"
	"math/rand"
	"strings"
	"time"
)

/*ClusterData represents all available(enabled, has networks e.t.c) services and its NFS and ISCSI volumes on cluster
or among the listed in serviceFilter (if serviceFilter option specified)*/
type ClusterData struct {
	ServicesData []ServiceData
}

func (clusterData *ClusterData) FindServiceDataByVolumeID(volumeID IVolumeId) (*ServiceData, error) {

	if len(clusterData.ServicesData) == 0 {
		return nil, &errors.EdgefsError{fmt.Sprintf("No Edgefs services available along Edgefs cluster"), errors.EdgefsNoServiceAvailableErrorCode}
	}

	clusterServiceData := clusterData.ServicesData
	// serviceName defined in VolumeID, then we need to check volume existance for this service
	var serviceDataFoundByName *ServiceData
	if volumeID.GetServiceName() != "" {
		serviceData, err := clusterData.FindServiceDataByServiceName(volumeID.GetServiceName())
		if err != nil {
			return nil, err
		}
		if serviceData != nil {
			serviceDataFoundByName = serviceData
			clusterServiceData = []ServiceData{*serviceData}
		} else {
			return nil, fmt.Errorf("Service data ptr is nil for service %s ", volumeID.GetServiceName())
		}
	}

	// No ServiceName defined in VolumeID, need to iterate over all available services
	for _, serviceData := range clusterServiceData {
		// find Edgefs Volume by VolumeID
		volume, err := serviceData.GetEdgefsVolume(volumeID)
		if err == nil && volume != nil {
			return &serviceData, nil
		}
	}
	return serviceDataFoundByName, &errors.EdgefsError{fmt.Sprintf("Volume '%s' not found in cluster ", volumeID.GetObjectPath()), errors.EdgefsVolumeNotExistsErrorCode}
}

func (clusterData *ClusterData) FindServiceDataByServiceName(serviceName string) (serviceData *ServiceData, err error) {
	for _, serviceData := range clusterData.ServicesData {
		if serviceName == serviceData.GetService().GetName() {
			return &serviceData, nil
		}
	}
	return nil, &errors.EdgefsError{fmt.Sprintf("Service '%s' not found! Check serviceFilter option or service status.", serviceName), errors.EdgefsServiceNotExistsErrorCode}
}

/* ClusterData Filters section */

/* template method to implement different Nfs service balancing types */
type serviceSelectorFunc func(clusterData *ClusterData) (*ServiceData, error)

func processServiceSelectionPolicy(serviceSelector serviceSelectorFunc, clusterData *ClusterData) (*ServiceData, error) {
	return serviceSelector(clusterData)
}

/* selects service with minimal export count from whole cluster (if serviceFilter ommited) or from serviceFilter's services */
func minimalExportsServiceSelector(clusterData *ClusterData) (*ServiceData, error) {
	if len(clusterData.ServicesData) > 0 {
		minServiceDataPtr := &clusterData.ServicesData[0]
		for _, data := range clusterData.ServicesData[1:] {
			currentValue := data.GetVolumesCount()
			if minServiceDataPtr.GetVolumesCount() > currentValue {
				minServiceDataPtr = &data
			}
		}

		return minServiceDataPtr, nil
	}

	return nil, fmt.Errorf("No Services available along edgefs cluster")
}

/* selects random service from whole cluster (if serviceFilter ommited) or from serviceFilter's services */
func randomServiceSelector(clusterData *ClusterData) (*ServiceData, error) {
	if len(clusterData.ServicesData) > 0 {
		rand.Seed(time.Now().Unix())
		randomIndex := rand.Intn(len(clusterData.ServicesData) - 1)
		return &clusterData.ServicesData[randomIndex], nil
	}

	return nil, &errors.EdgefsError{fmt.Sprintf("No Edgefs services available along edgefs cluster"), errors.EdgefsNoServiceAvailableErrorCode}
}

/*FindApropriateService find service with balancing policy*/
func (clusterData *ClusterData) FindApropriateServiceData(serviceBalancingPolicy string) (*ServiceData, error) {
	var serviceSelector serviceSelectorFunc
	switch strings.ToLower(serviceBalancingPolicy) {
	// minServicePolicy
	case "minexportspolicy":
		//log.Debugf("BalancerPolicy::MinimalExportsServiceSelector selected")
		serviceSelector = minimalExportsServiceSelector
	// randomServicePolicy
	case "randomservicepolicy":
		//log.Debugf("BalancerPolicy::RandomServiceSelector selected")
		serviceSelector = randomServiceSelector
	default:
		//log.Debugf("BalancerPolicy::MinimalExportsServiceSelector selected")
		serviceSelector = minimalExportsServiceSelector
	}

	return processServiceSelectionPolicy(serviceSelector, clusterData)
}
