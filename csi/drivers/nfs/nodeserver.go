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
	"os"
	"strings"

	client "../../edgefs"
	"github.com/container-storage-interface/spec/lib/go/csi"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/kubernetes/pkg/util/mount"
)

type NodeServer struct {
	NodeID string
	Logger *log.Entry
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

	clusterConfig, err := client.LoadEdgefsClusterConfig(client.EdgefsServiceType_NFS)
        if err != nil {
                l.Errorf("failed to read config file.  Error: %+v", err)
                return nil, status.Error(codes.Internal, err.Error())
        }

        volumeID, err := client.ParseNfsVolumeID(volumeIDStr, &clusterConfig)
        if err != nil {
                l.Errorf("Couldn't ParseNfsVolumeID Error: %s", err)
                return nil, status.Error(codes.Internal, err.Error())
        }

	edgefs, err := client.InitEdgeFS(&clusterConfig, client.EdgefsServiceType_NFS, volumeID.Segment, ns.Logger)
	if err != nil {
		l.Errorf("Failed to get EdgeFS instance, Error: %+v", err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	// get all services information to find already existing volume by path
	clusterData, err := edgefs.GetClusterData()
	if err != nil {
		l.Errorf("Couldn't get ClusterData Error: %s", err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	// find service to serve
	serviceData, err := clusterData.FindServiceDataByVolumeID(volumeID)
	if err != nil {
		l.Errorf("Can't find serviceData by volumeID  %+v. Error: %v", volumeID, err)
		return nil, status.Errorf(codes.NotFound, "Can't find service data by VolumeID:%s Error:%s", volumeID, err)
	}

	l.Debugf("Service %s found by volumeID %s", serviceData.GetService().GetName(), volumeID)

	mountParams, err := serviceData.GetEdgefsVolumeParams(volumeID)
	if err != nil {
		l.Errorf("Can't find serviceData by volumeID  %+v. Error: %v", volumeID, err)
		return nil, status.Errorf(codes.NotFound, "Can't get NFS mount point for  VoumeID:%s Error:%s", volumeID, err)
	}

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
		l.Warningf("Skipped to mount volume %s. Error: %s", targetPath, err)
		return &csi.NodePublishVolumeResponse{}, nil
	}

	fsType := req.GetVolumeCapability().GetMount().GetFsType()
	readOnly := req.GetReadonly()
	//attrib := req.GetVolumeAttributes()
	mountFlags := req.GetVolumeCapability().GetMount().GetMountFlags()

	mountOptions := edgefs.GetClusterConfig().GetMountOptions()
	if readOnly {
		if !contains(mountOptions, "ro") {
			mountOptions = append(mountOptions, "ro")
		}
	}

	l.Infof("target %v, fstype %v, readonly %v, mountflags %v", targetPath, fsType, readOnly, mountFlags)
	//l.Infof("EdgeFS export %s endpoint is %s", volID.FullObjectPath(), nfsEndpoint)
	mountPoint := mountParams["mountPoint"]
	err = mounter.Mount(mountPoint, targetPath, "nfs", mountOptions)
	if err != nil {
		if os.IsPermission(err) {
			l.Errorf("Failed to mount volume %s. Error: %v", mountPoint, err)
			return nil, status.Error(codes.PermissionDenied, err.Error())
		}
		if strings.Contains(err.Error(), "invalid argument") {
			l.Errorf("Failed to mount volume %s. Error: %v", mountPoint, err)
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		l.Errorf("Failed to mount volume %+v. Error: %v", mountPoint, err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	l.Infof("volumeID: %s, targetPath: %s, mountpoint: %s", volumeID, targetPath, mountPoint)
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

	err = mount.CleanupMountPoint(req.GetTargetPath(), mount.New(""), false)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
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
