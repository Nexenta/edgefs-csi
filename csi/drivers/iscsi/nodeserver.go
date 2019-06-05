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
	"os"
	"strconv"
	"strings"

	client "github.com/Nexenta/edgefs-csi/csi/edgefs"
	"github.com/container-storage-interface/spec/lib/go/csi"
	logrus "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/kubernetes/pkg/util/mount"
)

type NodeServer struct {
	NodeID string
	Logger *logrus.Entry
}

func (ns *NodeServer) NodeGetInfo(ctx context.Context, req *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	ns.Logger.WithField("func", "NodeGetInfo()").Infof("request: '%+v'", *req)
	return &csi.NodeGetInfoResponse{
		NodeId: ns.NodeID,
	}, nil
}

func (ns *NodeServer) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	l := ns.Logger.WithField("func", "NodePublishVolume()")
	l.Infof("request: '%+v'", *req)

	volumeIDStr := req.GetVolumeId()
	targetPath := req.GetTargetPath()

	// Check arguments
	if len(volumeIDStr) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume id must be provided")
	}
	if len(targetPath) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Target path must be provided")
	}

	clusterConfig, err := client.LoadEdgefsClusterConfig(client.EdgefsServiceType_ISCSI)
	if err != nil {
		l.Errorf("failed to read config file.  Error: %+v", err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	volumeID, err := client.ParseIscsiVolumeID(volumeIDStr, &clusterConfig)
	if err != nil {
		l.Errorf("Couldn't parse volumeID %s", err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	// Get EdgefsSegment node label, if exists use as volumeID' segment, for dynamic service' placement selection
	nodeSegmentName, err := client.Getk8sNodeLabel(ns.NodeID, clusterConfig.EdgefsSegmentNodeLabel)
	if err != nil {
		l.Errorf("Couldn't get node [%s] label `%s` value. Reason: %s", ns.NodeID, clusterConfig.EdgefsSegmentNodeLabel, err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	// force segment assignment in case of existing node labeling
	volumeSegment := volumeID.Segment
	if len(strings.TrimSpace(nodeSegmentName)) > 0 {
		l.Infof("Label `%s` exist for node [%s], set current segment as %s", clusterConfig.EdgefsSegmentNodeLabel, ns.NodeID, nodeSegmentName)
		volumeSegment = strings.TrimSpace(nodeSegmentName)
	}

	edgefs, err := client.InitEdgeFS(&clusterConfig, client.EdgefsServiceType_ISCSI, volumeSegment, ns.Logger)
	if err != nil {
		l.Errorf("Failed to get EdgeFS instance, Error: %+v", err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	// get all services information to find already existing volume by path
	clusterData, err := edgefs.GetClusterData()
	if err != nil {
		l.Errorf("Couldn't get ClusterData : %s", err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	//l.Infof("VolumeID: %s ClusterData: %+v", volumeID, clusterData)
	// find service to serve
	serviceData, err := clusterData.FindServiceDataByVolumeID(volumeID)
	if err != nil {
		l.Errorf("Can't find serviceData by volumeID  %+v. Error: %v", volumeID, err)
		return nil, status.Errorf(codes.NotFound, "Can't find service data by VolumeID:%s Error:%s", volumeID, err)
	}

	l.Infof("Service %s found by volumeID %s", serviceData.GetService().GetName(), volumeID)

	mountParams, err := serviceData.GetEdgefsVolumeParams(volumeID)
	if err != nil {
		l.Errorf("Can't find serviceData by volumeID  %+v. Error: %s", volumeID.GetObjectPath(), err)
		return nil, status.Errorf(codes.NotFound, "Can't get ISCSI volume params for VolumeID:%s Error:%s", volumeID.GetObjectPath(), err)
	}

	l.Debugf("VolumeMountParams: '%+v'", mountParams)

	fsType := req.GetVolumeCapability().GetMount().GetFsType()
	if fsType == "" {
		fsType = "ext4"
	}

	lunNumber, err := strconv.ParseInt(mountParams["lunNumber"], 10, 32)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "Failed to convert %s to Lun number Error:%s", mountParams["lun"], err)
	}
	lunNumber32 := int32(lunNumber)

	vpi := VolumePublishInfo{
		FilesystemType: fsType,
		VolumeAccessInfo: VolumeAccessInfo{
			IscsiAccessInfo: IscsiAccessInfo{
				IscsiTargetPortal: mountParams["portal"],
				IscsiTargetIQN:    mountParams["iqn"],
				IscsiPortals:      []string{},
				IscsiLunNumber:    lunNumber32,
			},
		},
	}

	l.Infof("VolumePublishInfo: '%+v'", vpi)
	// do not pass targetpath to AttachISCSIVolume to prevent mount
	err = AttachISCSIVolume(volumeID.GetObjectPath(), "", &vpi, ns.Logger)
	if err != nil {
		l.Errorf("AttachVolume error: %s", err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	if len(vpi.DevicePath) == 0 {
		l.Errorf("AttachVolume has no specified device name! error: %s", err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	l.Infof("VolumeID %s assigned to %s device", volumeID.GetObjectPath(), vpi.DevicePath)

	mounter := mount.New("")
	notMnt, err := mounter.IsLikelyNotMountPoint(targetPath)
	if err != nil {
		if os.IsNotExist(err) {
			if err := os.MkdirAll(targetPath, 0750); err != nil {
				l.Errorf("Failed to mkdir to target path %s. Error: %s", targetPath, err)
				return nil, status.Error(codes.Internal, err.Error())
			}
			notMnt = true
		} else {
			l.Errorf("Failed to mkdir to target path %s. Error: %s", targetPath, err)
			return nil, status.Error(codes.Internal, err.Error())
		}
	}

	if !notMnt {
		//l.Info("notMnt is False skipping")
		l.Warningf("Skipped to mount volume %s. Error: %s", targetPath, err)
		return &csi.NodePublishVolumeResponse{}, nil
	}

	readOnly := req.GetReadonly()
	mountFlags := req.GetVolumeCapability().GetMount().GetMountFlags()

	mountOptions := edgefs.GetClusterConfig().GetMountOptions()
	if readOnly {
		if !contains(mountOptions, "ro") {
			mountOptions = append(mountOptions, "ro")
		}
	}

	l.Infof("target %v, fstype %v, readonly %v, mountflags %v", targetPath, fsType, readOnly, mountFlags)
	err = mounter.Mount(vpi.DevicePath, targetPath, fsType, []string{})
	if err != nil {
		if os.IsPermission(err) {
			l.Errorf("Failed to mount device %s. Error: %v", vpi.DevicePath, err)
			return nil, status.Error(codes.PermissionDenied, err.Error())
		}
		if strings.Contains(err.Error(), "invalid argument") {
			l.Errorf("Failed to mount device %s. Error: %v", vpi.DevicePath, err)
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		l.Errorf("Failed to mount device %+v. Error: %v", vpi.DevicePath, err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	l.Infof("volumeID: %s, targetPath: %s, DevicePath: %s", volumeID, targetPath, vpi.DevicePath)
	return &csi.NodePublishVolumeResponse{}, nil
}

func (ns *NodeServer) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	l := ns.Logger.WithField("func", "NodeUnpublishVolume()")
	l.Infof("request: '%+v'", *req)

	targetPath := req.GetTargetPath()
	notMnt, err := mount.New("").IsLikelyNotMountPoint(targetPath)

	if err != nil {
		if os.IsNotExist(err) {
			return nil, status.Error(codes.NotFound, "Targetpath not found")
		}
		return nil, status.Error(codes.Internal, err.Error())

	}
	if notMnt {
		return nil, status.Error(codes.NotFound, "Volume not mounted")
	}

	err = PrepareDeviceAtMountPathForRemoval(req.GetTargetPath(), true, ns.Logger)
	if err != nil {
		l.Warningf("PrepareDeviceAtMountPathForRemoval error status: %s", err)
	}

	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (ns *NodeServer) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	//return &csi.NodeUnstageVolumeResponse{}, nil

	return nil, status.Error(codes.Unimplemented, "")
}

func (ns *NodeServer) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	//return &csi.NodeStageVolumeResponse{}, nil
	return nil, status.Error(codes.Unimplemented, "")
}

func (ns *NodeServer) NodeExpandVolume(ctx context.Context, req *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	//return &csi.NodeExpandVolumeResponse{}, nil
	return nil, status.Error(codes.Unimplemented, "")
}

func (d *NodeServer) NodeGetCapabilities(ctx context.Context, req *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: []*csi.NodeServiceCapability{
			{
				Type: &csi.NodeServiceCapability_Rpc{
					Rpc: &csi.NodeServiceCapability_RPC{
						Type: csi.NodeServiceCapability_RPC_UNKNOWN,
					},
				},
			},
		},
	}, nil
}

func (c *NodeServer) NodeGetVolumeStats(ctx context.Context, req *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
	out := new(csi.NodeGetVolumeStatsResponse)
	return out, nil
}

//TODO should be moved to Utils
func contains(arr []string, tofind string) bool {
	for _, item := range arr {
		if item == tofind {
			return true
		}
	}
	return false
}
