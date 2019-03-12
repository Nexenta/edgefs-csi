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
	"strconv"
	"strings"
)

type EdgefsNfsVolume struct {
	Cluster   string
	Tenant    string
	Bucket    string
	MountPath string
}

type EdgefsIscsiVolume struct {
	Cluster string
	Tenant  string
	Bucket  string
	Object     string
	LunNumber     uint32
}

type IEdgefsVolume interface {
	GetObjectPath() string
}

func (vol *EdgefsNfsVolume) GetObjectPath() string {
	return fmt.Sprintf("%s/%s/%s", vol.Cluster, vol.Tenant, vol.Bucket)
}

func (vol *EdgefsNfsVolume) String() string {
	return fmt.Sprintf("{Cluster: %s, Tenant: %s, Bucket: %s, MountPath: %s}", vol.Cluster, vol.Tenant, vol.Bucket, vol.MountPath)
}

func (vol *EdgefsIscsiVolume) GetObjectPath() string {
	return fmt.Sprintf("%s/%s/%s/%s", vol.Cluster, vol.Tenant, vol.Bucket, vol.Object)
}

func (vol *EdgefsIscsiVolume) String() string {
	return fmt.Sprintf("{Cluster: %s, Tenant: %s, Bucket: %s, Object: %s, LunNumber: %d}", vol.Cluster, vol.Tenant, vol.Bucket, vol.Object, vol.LunNumber)
}

func NewEdgefsNfsVolume(cluster, tenant, bucket, mountPath string) *EdgefsNfsVolume {
	return &EdgefsNfsVolume{Cluster: cluster, Tenant: tenant, Bucket: bucket, MountPath: mountPath}
}

func NewEdgefsIscsiVolume(cluster, tenant, bucket, object string, lunNumber uint32) *EdgefsIscsiVolume {
	return &EdgefsIscsiVolume{Cluster: cluster,
		Tenant: tenant,
		Bucket: bucket,
		Object:    object,
		LunNumber:     lunNumber}
}

/*ParseEdgefsIscsiVolumeStr parses volume string : 1@cltest/test/bk1/lun1 */
func ParseEdgefsIscsiVolumeStr(volume string) (*EdgefsIscsiVolume, error) {
	var objectParts = strings.Split(volume, "@")
	if len(objectParts) == 2 {
		lunNumber, err := strconv.ParseInt(objectParts[0], 10, 32)
		if err != nil {
			return nil, fmt.Errorf("Can't cast to int ISCSI volume LunId %s", objectParts[0])
		}
		parts := strings.Split(objectParts[1], "/")
		if len(parts) == 4 {
			return NewEdgefsIscsiVolume(parts[0], parts[1], parts[2], parts[3], uint32(lunNumber)), nil
		}
	}
	return nil, fmt.Errorf("Wrong ISCSI volume format for %s", volume)
}

/* ParseEdgefsNfsVolumeStr parses volume string 2,test/nfs-test@cltest/test/nfs-test */
func ParseEdgefsNfsVolumeStr(volume string) (*EdgefsNfsVolume, error) {
	var objectParts = strings.Split(volume, ",")
	if len(objectParts) == 2 {
		parts := strings.Split(objectParts[1], "@")
		if len(parts) == 2 {
			pathParts := strings.Split(parts[1], "/")
			if len(pathParts) == 3 {
				mountPath := "/" + parts[0]
				return NewEdgefsNfsVolume(pathParts[0], pathParts[1], pathParts[2], mountPath), nil
			}
		}
	}
	return nil, fmt.Errorf("Wrong NFS volume format for %s", volume)
}
