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
	"fmt"
	"strings"
)

type SnapshotInfo struct {
	SnapshotPath string
	SourceVolume string
	CreationTime int64
}

type CloneInfo struct {
	SourceSnapshotPath string
	SourceObjectPath   string
	CloneObjectPath    string
}

type ISnapshotId interface {
	GetObjectPath() string
	GetName() string
	Validate() error
}

type IscsiSnapshotId struct {
	Cluster string
	Tenant  string
	Bucket  string
	Object  string
	Name    string
}

func (snap *IscsiSnapshotId) GetObjectPath() string {
	return fmt.Sprintf("%s/%s/%s/%s", snap.Cluster, snap.Tenant, snap.Bucket, snap.Object)
}

func (snap *IscsiSnapshotId) Validate() error {
	missed := make([]string, 0)

	if snap.Cluster == "" {
		missed = append(missed, "Cluster")
	}
	if snap.Tenant == "" {
		missed = append(missed, "Tenant")
	}
	if snap.Bucket == "" {
		missed = append(missed, "Bucket")
	}
	if snap.Object == "" {
		missed = append(missed, "Object")
	}
	if snap.Name == "" {
		missed = append(missed, "Name")
	}

	if len(missed) > 0 {
		return fmt.Errorf("Those parameter(s) are missed %s", strings.Join(missed, ","))
	}
	return nil
}

func ParseIscsiSnapshotID(snapshotIDStr string, clusterConfig *EdgefsClusterConfig) (IscsiSnapshotId, error) {

	parts := strings.Split(snapshotIDStr, "@")
	if len(parts) != 2 {
		return IscsiSnapshotId{}, fmt.Errorf("Couldn't parse string %s as snapshotId. Example cluster/teant/bucket/object@snapshot-name", snapshotIDStr)
	}

	objectPathParts := strings.Split(parts[0], "/")

	snapshotID := IscsiSnapshotId{}
	snapshotID.Name = parts[1]

	// Object name only i.e snapshotID is object@snapname
	if len(objectPathParts) == 1 {
		if len(clusterConfig.Cluster) > 0 {
			snapshotID.Cluster = clusterConfig.Cluster
		}

		if len(clusterConfig.Tenant) > 0 {
			snapshotID.Tenant = clusterConfig.Tenant
		}

		if len(clusterConfig.Bucket) > 0 {
			snapshotID.Bucket = clusterConfig.Bucket
		}

		snapshotID.Object = objectPathParts[0]
	} else if len(objectPathParts) == 2 {
		// bucket/object@snapname
		if len(clusterConfig.Cluster) > 0 {
			snapshotID.Cluster = clusterConfig.Cluster
		}

		if len(clusterConfig.Tenant) > 0 {
			snapshotID.Tenant = clusterConfig.Tenant
		}

		snapshotID.Bucket = objectPathParts[0]
		if snapshotID.Bucket == "" {
			if len(clusterConfig.Bucket) > 0 {
				snapshotID.Bucket = clusterConfig.Bucket
			}
		}

		snapshotID.Object = objectPathParts[1]
	} else if len(objectPathParts) == 3 {
		// tenant/bucket/object@snapname

		if len(clusterConfig.Cluster) > 0  {
			snapshotID.Cluster = clusterConfig.Cluster
		}

		snapshotID.Tenant = objectPathParts[0]
		if snapshotID.Tenant == "" {
			if len(clusterConfig.Tenant) > 0  {
				snapshotID.Tenant = clusterConfig.Tenant
			}
		}

		snapshotID.Bucket = objectPathParts[1]
		if snapshotID.Bucket == "" {
			if len(clusterConfig.Bucket) > 0 {
				snapshotID.Bucket = clusterConfig.Bucket
			}
		}

		snapshotID.Object = objectPathParts[2]
	} else {
		// cluster/tenant/bucket/object@snapname
		//Cluster
		snapshotID.Cluster = objectPathParts[0]
		if snapshotID.Cluster == "" {
			if len(clusterConfig.Cluster) > 0 {
				snapshotID.Cluster = clusterConfig.Cluster
			}
		}
		//Tenant
		snapshotID.Tenant = objectPathParts[1]
		if snapshotID.Tenant == "" {
			if len(clusterConfig.Tenant) > 0 {
				snapshotID.Tenant = clusterConfig.Tenant
			}
		}
		//Bucket
		snapshotID.Bucket = objectPathParts[2]
		if snapshotID.Bucket == "" {
			if len(clusterConfig.Bucket) > 0 {
				snapshotID.Bucket = clusterConfig.Bucket
			}
		}
		snapshotID.Object = objectPathParts[3]
	}

	return snapshotID, nil
}
