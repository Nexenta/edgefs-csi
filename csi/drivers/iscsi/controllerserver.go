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
package iscsi

import (
	"fmt"
	"strconv"
	"strings"

	client "../../edgefs"
	csi "github.com/container-storage-interface/spec/lib/go/csi"
	timestamp "github.com/golang/protobuf/ptypes/timestamp"
	"github.com/pborman/uuid"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	logrus "github.com/sirupsen/logrus"
)

// supportedControllerCapabilities - driver controller capabilities
var supportedControllerCapabilities = []csi.ControllerServiceCapability_RPC_Type{
	csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
	csi.ControllerServiceCapability_RPC_LIST_VOLUMES,
	csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT,
	csi.ControllerServiceCapability_RPC_LIST_SNAPSHOTS,
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
	Logger *logrus.Entry
}

func (cs *ControllerServer) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	l := cs.Logger.WithField("func", "CreateVolume()")
	l.Infof("request: '%+v'", *req)

	edgefs, err := client.InitEdgeFS(client.EdgefsServiceType_ISCSI, cs.Logger)
	if err != nil {
		l.Fatal("Failed to get EdgeFS instance")
		return nil, err
	}

	// Volume Name
	volumeName := req.GetName()
	if len(volumeName) == 0 {
		volumeName = "csi-iscsi-volume-" + uuid.NewUUID().String()
	}

	params := req.GetParameters()
	// prevent null poiner if no parameters passed
	if params == nil {
		params = make(map[string]string)
	}

	// get volume size, 10Gb if not specified
	requiredBytes := req.GetCapacityRange().GetRequiredBytes()
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

	if bucket, ok := params["bucket"]; ok {
		volumePath += fmt.Sprintf("%s/", bucket)
	}

	volumePath += volumeName

	var sourceSnapshotID string
	if volumeContentSource := req.GetVolumeContentSource(); volumeContentSource != nil {
		if sourceSnapshot := volumeContentSource.GetSnapshot(); sourceSnapshot != nil {
			sourceSnapshotID = sourceSnapshot.GetSnapshotId()
		} else {
			return nil, status.Errorf(
				codes.InvalidArgument,
				"Only snapshots are supported as volume content source, but got type: %s",
				volumeContentSource.GetType(),
			)
		}
	}

	l.Info("volumePath: ", volumePath)
	newVolumeID, err := edgefs.CreateIscsiVolume(volumePath, sourceSnapshotID, requiredBytes, params)
	if err != nil {
		l.Errorf("Failed to CreateVolume %s: %v", volumePath, err)
		return nil, err
	}

	// CreateVolume response
	resp := &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      newVolumeID,
			CapacityBytes: requiredBytes,
			//Attributes:    req.GetParameters(),
		},
	}
	l.Debugf("response: '%+v'", *resp)
	return resp, nil
}

func (cs *ControllerServer) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	l := cs.Logger.WithField("func", "DeleteVolume()")
	l.Infof("request: '%+v'", *req)

	edgefs, err := client.InitEdgeFS(client.EdgefsServiceType_ISCSI, cs.Logger)
	if err != nil {
		l.Fatal("Failed to get EdgeFS instance")
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
			l.Infof("DeleteVolume:IsVolumeExist volume %s does not exist", volumeID)
			return &csi.DeleteVolumeResponse{}, nil
		}
	*/

	err = edgefs.DeleteIscsiVolume(volumeID)
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

// GetCapacity - not implemented
func (cs *ControllerServer) GetCapacity(ctx context.Context, req *csi.GetCapacityRequest) (
	*csi.GetCapacityResponse,
	error,
) {
	cs.Logger.WithField("func", "GetCapacity()").Infof("request: '%+v'", *req)
	return nil, status.Error(codes.Unimplemented, "")
}

func (cs *ControllerServer) ListVolumes(ctx context.Context, req *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	l := cs.Logger.WithField("func", "ListVolumes()")
	l.Infof("request: '%+v'", *req)

	edgefs, err := client.InitEdgeFS(client.EdgefsServiceType_ISCSI, cs.Logger)
	if err != nil {
		l.Fatalf("Failed to get EdgeFS instance. Error:", err)
		return nil, err
	}

	volumes, err := edgefs.ListVolumes()
	l.Info("volumes: '%+v' ", volumes)

	entries := make([]*csi.ListVolumesResponse_Entry, len(volumes))
	for i, v := range volumes {
		// Initialize entry
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

func (cs *ControllerServer) CreateSnapshot(ctx context.Context, req *csi.CreateSnapshotRequest) (
	*csi.CreateSnapshotResponse,
	error,
) {
	l := cs.Logger.WithField("func", "CreateSnapshot()")
	l.Infof("request: '%+v'", *req)

	edgefs, err := client.InitEdgeFS(client.EdgefsServiceType_ISCSI, cs.Logger)
	if err != nil {
		l.Fatalf("Failed to get EdgeFS instance. Error:", err)
		return nil, err
	}

	volumePath := req.GetSourceVolumeId()
	if len(volumePath) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Snapshot source volume ID must be provided")
	}

	name := req.GetName()
	if len(name) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Snapshot name must be provided")
	}

	snapInfo, err := edgefs.CreateObjectSnapshot(volumePath, name)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "CreateObjectSnapshot failed: %s", err.Error())
	}

	l.Debugf("snapInfo: %+v", snapInfo)
	res := &csi.CreateSnapshotResponse{
		Snapshot: &csi.Snapshot{
			SnapshotId:     snapInfo.SnapshotPath,
			SourceVolumeId: volumePath,
			CreationTime: &timestamp.Timestamp{
				Seconds: snapInfo.CreationTime,
			},
			ReadyToUse: true, //TODO use actual state
			//SizeByte: 0 //TODO size of zero means it is unspecified
		},
	}

	l.Infof("response: '%+v'", *res)
	return res, nil
}

func (cs *ControllerServer) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (
	*csi.DeleteSnapshotResponse,
	error,
) {
	l := cs.Logger.WithField("func", "DeleteSnapshot()")
	l.Infof("request: '%+v'", *req)

	edgefs, err := client.InitEdgeFS(client.EdgefsServiceType_ISCSI, cs.Logger)
	if err != nil {
		l.Fatalf("Failed to get EdgeFS instance. Error:", err)
		return nil, err
	}

	snapshotPath := req.GetSnapshotId()
	if len(snapshotPath) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Snapshot ID must be provided")
	}

	err = edgefs.DeleteObjectSnapshot(snapshotPath)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "DeleteSnapshot failed: %s", err.Error())
	}

	return &csi.DeleteSnapshotResponse{}, nil
}

func (cs *ControllerServer) ListSnapshots(ctx context.Context, req *csi.ListSnapshotsRequest) (
	*csi.ListSnapshotsResponse,
	error,
) {
	l := cs.Logger.WithField("func", "ListSnapshots()")
	l.Infof("request: '%+v'", *req)

	edgefs, err := client.InitEdgeFS(client.EdgefsServiceType_ISCSI, cs.Logger)
	if err != nil {
		l.Fatalf("Failed to get EdgeFS instance. Error:", err)
		return nil, err
	}

	volumePath := req.GetSnapshotId()
	if volumePath == "" {
		volumePath = req.GetSourceVolumeId()
	}

	volumeParts := strings.Split(volumePath, "@")
	if len(volumeParts) > 1 {
		volumePath = volumeParts[0]
	}

	nextToken := ""
	//startingToken := req.GetStartingToken()
	//maxEntries := req.GetMaxEntries()
	snapshots, err := edgefs.ListObjectSnapshots(volumePath, volumeParts[0])
	if err != nil {
		return nil, status.Errorf(codes.Internal, "ListSnapshots failed: %s", err.Error())
	}

	snapshotEntries := []*csi.ListSnapshotsResponse_Entry{}
	for _, snap := range snapshots {
		snapshotEntries = append(snapshotEntries, &csi.ListSnapshotsResponse_Entry{
			Snapshot: &csi.Snapshot{
				SnapshotId:     snap.SnapshotPath,
				SourceVolumeId: snap.SourceVolume,
				CreationTime: &timestamp.Timestamp{
					Seconds: snap.CreationTime,
				},
				ReadyToUse: true, //TODO use actual state
			},
		})
	}
	response := &csi.ListSnapshotsResponse{
		Entries:   snapshotEntries,
		NextToken: nextToken,
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

	l.Infof("volume %s VolumeCapabilities: %+v, Context: %+v", volumePath, volumeCapabilities, volumeContext)
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
	// only block is supported
	if requestedVolumeCapability.GetMount() != nil {
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
