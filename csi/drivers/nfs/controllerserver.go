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
package nfs

import (
	"fmt"
	"strconv"

	client "../../edgefs"
	csi "github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	defaultNFSVolumeQuota int64 = 1073741824
)

// supportedControllerCapabilities - driver controller capabilities
var supportedControllerCapabilities = []csi.ControllerServiceCapability_RPC_Type{
	csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
	csi.ControllerServiceCapability_RPC_LIST_VOLUMES,
}

// supportedVolumeCapabilities - driver volume capabilities
var supportedVolumeCapabilities = []*csi.VolumeCapability{
	&csi.VolumeCapability{
		AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY},
	},
	&csi.VolumeCapability{
		AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
	},
	&csi.VolumeCapability{
		AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY},
	},
	&csi.VolumeCapability{
		AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_SINGLE_WRITER},
	},
	&csi.VolumeCapability{
		AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER},
	},
}

type ControllerServer struct {
	Logger *log.Entry
}

func (cs *ControllerServer) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	l := cs.Logger.WithField("func", "CreateVolume()")
	l.Infof("req: '%+v'", *req)

	// Volume Name
	volumeName := req.GetName()
	if len(volumeName) == 0 {
		volumeName = "csi-nfs-volume-" + uuid.NewUUID().String()
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

	clusterConfig, err := client.LoadEdgefsClusterConfig(client.EdgefsServiceType_NFS)
	if err != nil {
		err = fmt.Errorf("failed to read config file.  Error: %+v", err)
		l.WithField("func", "LoadEdgefsClusterConfig()").Errorf("%+v", err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	// Get segment value from parameters or if not defined get default one
	segment, err := clusterConfig.GetSegment(params["segment"])
	if err != nil {
		l.WithField("func", "GetSegment()").Warningf("%+v", err)
	}

	// Init Edgefs struct  and discovery edgefs services in segment
	edgefs, err := client.InitEdgeFS(&clusterConfig, client.EdgefsServiceType_NFS, segment, cs.Logger)
	if err != nil {
		l.WithField("func", "InitEdgeFS()").Errorf("Failed to get EdgeFS instance, %s", err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	volumePath := ""
	// add current segment to volume path
	volumePath += segment

	if service, ok := params["service"]; ok {
		volumePath += fmt.Sprintf(":%s@", service)
	} else {
		volumePath += "@"
	}

	if cluster, ok := params["cluster"]; ok {
		volumePath += fmt.Sprintf("%s/", cluster)
	}

	if tenant, ok := params["tenant"]; ok {
		volumePath += fmt.Sprintf("%s/", tenant)
	}
	volumePath += volumeName

	l.Infof("volumePath: %s", volumePath)
	newVolumeID, err := edgefs.CreateNfsVolume(volumePath, 0, params)
	if err != nil {
		l.Errorf("Failed to create NFS volume %s: %s", volumePath, err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	// CreateVolume response
	resp := &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      newVolumeID,
			CapacityBytes: requiredBytes,
		},
	}
	l.Debugf("response: '%+v'", *resp)
	return resp, nil
}

func (cs *ControllerServer) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	l := cs.Logger.WithField("func", "DeleteVolume()")
	l.Infof("req: '%+v'", *req)

	// VolumeID
	csiVolumeID := req.GetVolumeId()
	if len(req.GetVolumeId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume id must be provided")
	}

	clusterConfig, err := client.LoadEdgefsClusterConfig(client.EdgefsServiceType_NFS)
	if err != nil {
		err = fmt.Errorf("failed to read config file.  Error: %+v", err)
		l.WithField("func", "DeleteVolume()").Errorf("%+v", err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	volumeID, err := client.ParseNfsVolumeID(csiVolumeID, &clusterConfig)
	if err != nil {
		err = fmt.Errorf("Can't parse csi volumeID %s.  Error: %+v", csiVolumeID, err)
		l.WithField("func", "DeleteVolume)").Errorf("%+v", err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	edgefs, err := client.InitEdgeFS(&clusterConfig, client.EdgefsServiceType_NFS, volumeID.Segment, cs.Logger)
	if err != nil {
		l.WithField("func", "InitEdgeFS()").Errorf("Failed to get EdgeFS instance, %s", err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	// If the volume is not found, then we can return OK
	/*
		if edgefs.IsVolumeExist(volumeID) == false {
			log.Infof("DeleteVolume:IsVolumeExist volume %s does not exist", volumeID)
			return &csi.DeleteVolumeResponse{}, nil
		}
	*/

	err = edgefs.DeleteNfsVolume(volumeID)
	if err != nil {
		e := fmt.Sprintf("Unable to delete volume with id %s: %s",
			req.GetVolumeId(),
			err.Error())
		l.Errorln(e)
		return nil, status.Error(codes.Internal, e)
	}

	return &csi.DeleteVolumeResponse{}, nil
}

func (cs *ControllerServer) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	cs.Logger.WithField("func", "ControllerPublishVolume()").Infof("request: '%+v'", *req)
	return &csi.ControllerPublishVolumeResponse{}, nil
}

func (cs *ControllerServer) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	cs.Logger.WithField("func", "ControllerUnpublishVolume()").Infof("request: '%+v'", *req)
	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

func (cs *ControllerServer) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	cs.Logger.WithField("func", "ControllerExpandVolume()").Infof("request: '%+v'", *req)
	return &csi.ControllerExpandVolumeResponse{}, nil
}

func (cs *ControllerServer) CreateSnapshot(ctx context.Context, req *csi.CreateSnapshotRequest) (
	*csi.CreateSnapshotResponse,
	error,
) {
	cs.Logger.WithField("func", "CreateSnapshot()").Infof("request: '%+v'", *req)
	return nil, status.Error(codes.Unimplemented, "")
}

func (cs *ControllerServer) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (
	*csi.DeleteSnapshotResponse,
	error,
) {
	cs.Logger.WithField("func", "DeleteSnapshot()").Infof("request: '%+v'", *req)
	return nil, status.Error(codes.Unimplemented, "")
}

func (cs *ControllerServer) ListSnapshots(ctx context.Context, req *csi.ListSnapshotsRequest) (
	*csi.ListSnapshotsResponse,
	error,
) {
	cs.Logger.WithField("func", "ListSnapshots()").Infof("request: '%+v'", *req)
	return nil, status.Error(codes.Unimplemented, "")
}

// GetCapacity - not implemented
func (cs *ControllerServer) GetCapacity(ctx context.Context, req *csi.GetCapacityRequest) (
	*csi.GetCapacityResponse,
	error,
) {
	cs.Logger.WithField("func", "GetCapacity()").Infof("request: '%+v'", *req)
	return nil, status.Error(codes.Unimplemented, "")
}

func (cs *ControllerServer) ListVolumes(ctx context.Context, req *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	l := cs.Logger.WithField("func", "GetCapacity()")
	l.Infof("req: '%+v'", *req)

	clusterConfig, err := client.LoadEdgefsClusterConfig(client.EdgefsServiceType_NFS)
	if err != nil {
		err = fmt.Errorf("failed to read config file.  Error: %+v", err)
		l.WithField("func", "DeleteVolume()").Errorf("%+v", err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	edgefs, err := client.InitEdgeFS(&clusterConfig, client.EdgefsServiceType_NFS, "", cs.Logger)
	if err != nil {
		l.Fatalf("Failed to get EdgeFS instance. Error:", err)
		return nil, err
	}

	volumes, err := edgefs.ListVolumes()
	if err != nil {
		l.Fatalf("Failed to list EdgeFS volumes. Error:", err)
		return nil, err
	}

	l.Debugf("Volumes: %+v", volumes)

	entries := make([]*csi.ListVolumesResponse_Entry, len(volumes))
	for i, v := range volumes {
		// Initialize entry
		l.Debugf("VolumeObjectPath: %s", v.GetObjectPath())
		entries[i] = &csi.ListVolumesResponse_Entry{
			Volume: &csi.Volume{VolumeId: v.GetObjectPath()},
		}
	}

	response := &csi.ListVolumesResponse{
		Entries: entries,
	}
	l.Infof("response: '%+v'", *response)

	return response, nil
}

func (cs *ControllerServer) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	l := cs.Logger.WithField("func", "ValidateVolumeCapabilities()")
	l.Infof("request: '%+v'", *req)

	volumePath := req.GetVolumeId()
	if len(volumePath) == 0 {
		return nil, status.Error(codes.InvalidArgument, "req.VolumeId must be provided")
	}

	volumeCapabilities := req.GetVolumeCapabilities()
	if volumeCapabilities == nil {
		return nil, status.Error(codes.InvalidArgument, "req.VolumeCapabilities must be provided")
	}

	// volume attributes are passed from ControllerServer.CreateVolume()
	volumeContext := req.GetVolumeContext()

	for _, reqC := range volumeCapabilities {
		supported := validateVolumeCapability(reqC)
		l.Infof("requested capability: '%s', supported: %t", reqC.GetAccessMode().GetMode(), supported)
		if !supported {
			message := fmt.Sprintf("Driver does not support volume capability mode: %s", reqC.GetAccessMode().GetMode())
			l.Warn(message)
			return &csi.ValidateVolumeCapabilitiesResponse{
				Message: message,
			}, nil
		}
	}

	response := &csi.ValidateVolumeCapabilitiesResponse{
		Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
			VolumeCapabilities: supportedVolumeCapabilities,
			VolumeContext:      volumeContext,
		},
	}

	l.Infof("response: '%+v'", *response)
	return response, nil
}

func validateVolumeCapability(requestedVolumeCapability *csi.VolumeCapability) bool {
	// block is not supported
	if requestedVolumeCapability.GetBlock() != nil {
		return false
	}

	requestedMode := requestedVolumeCapability.GetAccessMode().GetMode()
	for _, volumeCapability := range supportedVolumeCapabilities {
		if volumeCapability.GetAccessMode().GetMode() == requestedMode {
			return true
		}
	}
	return false
}

// ControllerGetCapabilities - controller capabilities
func (cs *ControllerServer) ControllerGetCapabilities(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (
	*csi.ControllerGetCapabilitiesResponse,
	error,
) {
	cs.Logger.WithField("func", "ControllerGetCapabilities()").Infof("request: '%+v'", *req)

	var capabilities []*csi.ControllerServiceCapability
	for _, c := range supportedControllerCapabilities {
		capabilities = append(capabilities, newControllerServiceCapability(c))
	}

	return &csi.ControllerGetCapabilitiesResponse{
		Capabilities: capabilities,
	}, nil
}

func newControllerServiceCapability(cap csi.ControllerServiceCapability_RPC_Type) *csi.ControllerServiceCapability {
	return &csi.ControllerServiceCapability{
		Type: &csi.ControllerServiceCapability_Rpc{
			Rpc: &csi.ControllerServiceCapability_RPC{
				Type: cap,
			},
		},
	}
}
