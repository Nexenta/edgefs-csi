/*
 * Copyright (c) 2015-2019 Nexenta Systems, Inc.
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
	"../errors"
)

const (
	EdgefsServiceType_NFS   = "nfs"
	EdgefsServiceType_ISCSI = "iscsi"
)

/* represents Edgefs internal service objects */
type IEdgefsService interface {
	GetName() string
	GetType() string
	GetK8SSvcName() string
	GetK8SNamespace() string
	GetEntrypoint() string
	GetStatus() string

	SetName(value string)
	SetType(value string)
	SetK8SSvcName(value string)
	SetK8SNamespace(value string)
	SetEntrypoint(value string)
	SetStatus(value string)
}

type EdgefsService struct {
	// Edgefs service name
	Name string
	// Edgefs service type [nfs|iscsi]
	Type string
	// Nfs mount point for NFS or portal for ISCSI with port
	Entrypoint string
	// current Kubernetes Edgefs service namespace
	K8SNamespace string
	// Kubernetes Edgefs service name
	K8SSvcName string
	// service status [enabled|disabled]
	Status string
}

/* Getters */
func (es *EdgefsService) GetName() string         { return es.Name }
func (es *EdgefsService) GetType() string         { return es.Type }
func (es *EdgefsService) GetEntrypoint() string   { return es.Entrypoint }
func (es *EdgefsService) GetK8SSvcName() string   { return es.K8SSvcName }
func (es *EdgefsService) GetK8SNamespace() string { return es.K8SNamespace }
func (es *EdgefsService) GetStatus() string       { return es.Status }

/* Setters */
func (es *EdgefsService) SetName(value string)         { es.Name = value }
func (es *EdgefsService) SetType(value string)         { es.Type = value }
func (es *EdgefsService) SetEntrypoint(value string)   { es.Entrypoint = value }
func (es *EdgefsService) SetK8SSvcName(value string)   { es.K8SSvcName = value }
func (es *EdgefsService) SetK8SNamespace(value string) { es.K8SNamespace = value }
func (es *EdgefsService) SetStatus(value string)       { es.Status = value }

type EdgefsNfsService struct {
	EdgefsService
	/* additional Edgefs NFS service props */
}

/* String implements Stringer iface for EdgefsService */
func (es *EdgefsService) String() string {
	return fmt.Sprintf("{Name: %s, Type: %s, Entrypoint: %s, K8SSvcName: %s, K8SNamespace: %s, Status: %s}", es.Name,
		es.Type,
		es.Entrypoint,
		es.K8SSvcName,
		es.K8SNamespace,
		es.Status)
}

type EdgefsIscsiService struct {
	EdgefsService
	/* additional Edgefs ISCSI service props */
	TargetIqn   string
}

//IServiceData interface to operate different service types
type IServiceData interface {
	GetType() string
	GetService() IEdgefsService
	GetEdgefsVolume(volumeId IVolumeId) IEdgefsVolume
	GetVolumesCount() int
}

/* represents base Edgefs internal service data with related volumes */
type ServiceData struct {
	Service IEdgefsService
	Volumes []IEdgefsVolume
}

func (sd *ServiceData) GetType() string {
	return sd.Service.GetType()
}

func (sd *ServiceData) GetService() IEdgefsService {
	return sd.Service
}

func (sd *ServiceData) GetVolumesCount() int {
	return len(sd.Volumes)
}

func (sd *ServiceData) GetEdgefsVolume(volumeId IVolumeId) (IEdgefsVolume, error) {
	// just iterate over all Edgefs Volume and compare it by ObjectPath
	for _, volume := range sd.Volumes {
		if volume.GetObjectPath() == volumeId.GetObjectPath() {
			return volume, nil
		}
	}
	return nil, &errors.EdgefsError{fmt.Sprintf("Volume %s not found in service %s", volumeId.GetObjectPath(), sd.GetService().GetName()), errors.EdgefsVolumeNotExistsErrorCode}
}

func (sd *ServiceData) GetEdgefsVolumeParams(volumeId IVolumeId) (map[string]string, error) {

	resultMap := make(map[string]string)
	volume, err := sd.GetEdgefsVolume(volumeId)
	if err != nil {
		return nil, err
	}

	switch sd.GetType() {
	case EdgefsServiceType_NFS:
		if nfsService, ok := sd.GetService().(*EdgefsNfsService); ok {
			//TODO: Add type error handling
			if nfsVolume, ok := volume.(*EdgefsNfsVolume); ok {
				resultMap["mountPoint"] = fmt.Sprintf("%s:%s", nfsService.Entrypoint, nfsVolume.MountPath)
				return resultMap, nil
			} else {
				return nil, fmt.Errorf("Failed to cast volume %+v to EdgefsiNfsVolume", nfsVolume)
			}
		} else {
			return nil, fmt.Errorf("Failed to cast service %+v to EdgefsNfsService", sd.GetService())
		}
	case EdgefsServiceType_ISCSI:
		if iscsiService, ok := sd.GetService().(*EdgefsIscsiService); ok {
			//TODO: Add type error handling
			if iscsiVolume, ok := volume.(*EdgefsIscsiVolume); ok {
				resultMap["portal"] = iscsiService.Entrypoint
				resultMap["iqn"] = iscsiService.TargetIqn
				resultMap["lunNumber"] = fmt.Sprintf("%d", iscsiVolume.LunNumber)
				return resultMap, nil
			} else {
				return nil, fmt.Errorf("Failed to cast volume %+v to EdgefsIscsiVolume", iscsiVolume)
			}
		} else {
			return nil, fmt.Errorf("Failed to cast service %+v to EdgefsiIscsiService", sd.GetService())
		}
	default:
		return nil, fmt.Errorf("Unknown service type %s to get mountParameters", sd.GetType())
	}

	return nil, fmt.Errorf("Failed to get volume params from service %+v by VolumeID: %+v", sd.GetService(), volumeId)
}
