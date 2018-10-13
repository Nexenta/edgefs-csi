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
	"os"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi/v0"
	csicommon "github.com/kubernetes-csi/drivers/pkg/csi-common"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/kubernetes/pkg/util/mount"
	"k8s.io/kubernetes/pkg/volume/util"
)

type nodeServer struct {
	*csicommon.DefaultNodeServer
}

func (ns *nodeServer) NodeGetId(ctx context.Context, req *csi.NodeGetIdRequest) (*csi.NodeGetIdResponse, error) {
	log.Infof("NodeGetId req[%#v]", req)
	// Using default function
	log.Info("NodeGetId invoked")
	return ns.DefaultNodeServer.NodeGetId(ctx, req)
}

func (ns *nodeServer) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	log.Infof("NodeServer::NodePublishVolume req[%+v]", *req)
	edgefs, err := InitEdgeFS("NodeServer::NodePublishVolume")
	if err != nil {
		log.Fatal("Failed to get EdgeFS instance")
		return nil, err
	}

	//log.Info("NodeServer::NodePublishVolume:edgefs : %+v", edgefs)
	volumeID := req.GetVolumeId()
	targetPath := req.GetTargetPath()

	// Check arguments
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume id must be provided")
	}
	if len(targetPath) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Target path must be provided")
	}

	volID, clusterData, err := edgefs.GetClusterDataByVolumeID(volumeID)
	if err != nil {
		log.Errorf("NodeServer::NodePublishVolume Can't get clusterData by volumeID  %+v. Error: %v", volID, err)
		return nil, status.Errorf(codes.NotFound, "Can't get cluster information by volumeID:%s, Error:%s", volumeID, err)
	}
	//log.Infof("VolumeID: %s ClusterData: %+v", volumeID, clusterData)

	// find service to serve
	serviceData, err := clusterData.FindServiceDataByVolumeID(volID)
	if err != nil {
		log.Errorf("NodeServer::NodePublishVolume Can't find serviceData by volumeID  %+v. Error: %v", volID, err)
		return nil, status.Errorf(codes.NotFound, "Can't find service data by VolumeID:%s Error:%s", volID, err)
	}
	log.Infof("Service %s found by volumeID %s", serviceData.Service, volumeID)

	nfsVolume, nfsEndpoint, err := serviceData.GetNFSVolumeAndEndpoint(volID)
	if err != nil {
		log.Errorf("NodeServer::NodePublishVolume Can't find nfs volume %+v. Error: %v", volID, err)
		return nil, status.Errorf(codes.NotFound, "Can't find NFS Volume or endpoint by VolumeID:%s Error:%s", volID, err)
	}

	mounter := mount.New("")
	notMnt, err := mounter.IsLikelyNotMountPoint(targetPath)
	if err != nil {
		if os.IsNotExist(err) {
			if err := os.MkdirAll(targetPath, 0750); err != nil {
				log.Errorf("NodeServer::NodePublishVolume Failed to mkdir to target path %+v. Error: %v", nfsVolume, err)
				return nil, status.Error(codes.Internal, err.Error())
			}
			notMnt = true
		} else {
			log.Errorf("NodeServer::NodePublishVolume Failed to mkdir to target path %+v. Error: %v", nfsVolume, err)
			return nil, status.Error(codes.Internal, err.Error())
		}
	}

	if !notMnt {
		//log.Info("notMnt is False skipping")
		log.Warning("NodeServer::NodePublishVolume Skipped to mount volume %+v. Error: %v", nfsVolume, err)
		return &csi.NodePublishVolumeResponse{}, nil
	}

	fsType := req.GetVolumeCapability().GetMount().GetFsType()
	readOnly := req.GetReadonly()
	attrib := req.GetVolumeAttributes()
	mountFlags := req.GetVolumeCapability().GetMount().GetMountFlags()

	mountOptions := edgefs.GetClusterConfig().GetMountOptions()
	if readOnly {
		if !contains(mountOptions, "ro") {
			mountOptions = append(mountOptions, "ro")
		}
	}

	log.Infof("target %v\nfstype %v\nreadonly %v\nattributes %v\n mountflags %v\n", targetPath, fsType, readOnly, attrib, mountFlags)
	//log.Infof("EdgeFS export %s endpoint is %s", volID.FullObjectPath(), nfsEndpoint)

	err = mounter.Mount(nfsEndpoint, targetPath, "nfs", mountOptions)
	if err != nil {
		if os.IsPermission(err) {
			log.Errorf("NodeServer::NodePublishVolume Failed to mount volume %+v. Error: %v", nfsVolume, err)
			return nil, status.Error(codes.PermissionDenied, err.Error())
		}
		if strings.Contains(err.Error(), "invalid argument") {
			log.Errorf("NodeServer::NodePublishVolume Failed to mount volume %+v. Error: %v", nfsVolume, err)
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		log.Errorf("NodeServer::NodePublishVolume Failed to mount volume %+v. Error: %v", nfsVolume, err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	log.Infof("NodeServer::NodePublishVolume volumeID: %s, targetPath: %s, endpoint: %s", volID, targetPath, nfsEndpoint)
	return &csi.NodePublishVolumeResponse{}, nil
}

func (ns *nodeServer) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	log.Infof("NodeServer::NodeUnpublishVolume request[%+v]", *req)

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

	err = util.UnmountPath(req.GetTargetPath(), mount.New(""))
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (ns *nodeServer) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	//return &csi.NodeUnstageVolumeResponse{}, nil

	return nil, status.Error(codes.Unimplemented, "")
}

func (ns *nodeServer) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	//return &csi.NodeStageVolumeResponse{}, nil
	return nil, status.Error(codes.Unimplemented, "")
}

func (d *nodeServer) NodeGetCapabilities(ctx context.Context, req *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
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

func (c *nodeServer) NodeGetVolumeStats(ctx context.Context, req *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
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
