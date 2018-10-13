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

	csi "github.com/container-storage-interface/spec/lib/go/csi/v0"
	csicommon "github.com/kubernetes-csi/drivers/pkg/csi-common"
	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	defaultNFSVolumeQuota int64 = 1073741824
)

type controllerServer struct {
	*csicommon.DefaultControllerServer
}

func (cs *controllerServer) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	log.Infof("CreateVolume request[%+v]", *req)

	edgefs, err := InitEdgeFS("ControllerServer::CreateVolume")
	if err != nil {
		log.Fatal("Failed to get EdgeFS instance")
		return nil, err
	}

	// Volume Name
	volumeName := req.GetName()
	if len(volumeName) == 0 {
		volumeName = "csi-volume-" + uuid.NewUUID().String()
	}

	params := req.GetParameters()
	// prevent null poiner if no parameters passed
	if params == nil {
		params = make(map[string]string)
	}

	// get volume size, 1Gb if not specified
	requiredBytes := req.GetCapacityRange().GetRequiredBytes()
	if requiredBytes == 0 {
		requiredBytes = defaultNFSVolumeQuota
	}
	params["size"] = strconv.FormatInt(requiredBytes, 10)

	volumePath := ""
	if service, ok := params["service"]; ok {
		volumePath += fmt.Sprintf("%s@", service)
	}

	if cluster, ok := params["cluster"]; ok {
		volumePath += fmt.Sprintf("%s/", cluster)
	}

	if tenant, ok := params["tenant"]; ok {
		volumePath += fmt.Sprintf("%s/", tenant)
	}
	volumePath += volumeName

	log.Info("ControllerServer::CreateVolume : ", volumePath)
	newVolumeID, err := edgefs.CreateVolume(volumePath, 0, params)
	if err != nil {
		log.Errorf("ControllerServer::CreateVolume Failed to CreateVolume %s: %v", volumePath, err)
		return nil, err
	}

	// CreateVolume response
	resp := &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			Id:            newVolumeID,
			CapacityBytes: requiredBytes,
			Attributes:    req.GetParameters(),
		},
	}

	return resp, nil
}

func (cs *controllerServer) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	log.Infof("ControllerServer::DeleteVolume request[%+v]", *req)

	edgefs, err := InitEdgeFS("ControllerServer::DeleteVolume")
	if err != nil {
		log.Fatal("Failed to get EdgeFS instance")
		return nil, err
	}

	// VolumeID
	volumeID := req.GetVolumeId()
	if len(req.GetVolumeId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume id must be provided")
	}

	// If the volume is not found, then we can return OK
	/*
		if edgefs.IsVolumeExist(volumeID) == false {
			log.Infof("DeleteVolume:IsVolumeExist volume %s does not exist", volumeID)
			return &csi.DeleteVolumeResponse{}, nil
		}
	*/

	err = edgefs.DeleteVolume(volumeID)
	if err != nil {
		e := fmt.Sprintf("Unable to delete volume with id %s: %s",
			req.GetVolumeId(),
			err.Error())
		log.Errorln(e)
		return nil, status.Error(codes.Internal, e)
	}

	return &csi.DeleteVolumeResponse{}, nil
}

func (cs *controllerServer) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	log.Infof("ControllerServer::PublishVolume req[%+v]", *req)
	return &csi.ControllerPublishVolumeResponse{}, nil
}

func (cs *controllerServer) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	log.Infof("ControllerUnpublishVolume req[%#v]", req)
	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

func (cs *controllerServer) ListVolumes(ctx context.Context, req *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	log.Infof("ControllerServer::ListVolumes request[%+v]", *req)

	edgefs, err := InitEdgeFS("ControllerServer::ListVolumes")
	if err != nil {
		log.Fatalf("Failed to get EdgeFS instance. Error:", err)
		return nil, err
	}

	volumes, err := edgefs.ListVolumes()
	//log.Info("ControllerListVolumes ", volumes)

	entries := make([]*csi.ListVolumesResponse_Entry, len(volumes))
	for i, v := range volumes {
		// Initialize entry
		entries[i] = &csi.ListVolumesResponse_Entry{
			Volume: &csi.Volume{Id: v.VolumeID.FullObjectPath()},
		}
	}

	return &csi.ListVolumesResponse{
		Entries: entries,
	}, nil
}

func (cs *controllerServer) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	for _, cap := range req.VolumeCapabilities {
		if cap.GetBlock() != nil {
			return &csi.ValidateVolumeCapabilitiesResponse{Supported: false, Message: ""}, nil
		}
	}
	return &csi.ValidateVolumeCapabilitiesResponse{Supported: true}, nil
}
