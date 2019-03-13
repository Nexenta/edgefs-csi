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
	"strings"
)

type IVolumeId interface {
	GetServiceName() string
	SetServiceName(name string)
	GetObjectPath() string
	Validate() error
}

type NfsVolumeId struct {
	Service string
	Cluster string
	Tenant  string
	Bucket  string
}

func (nfsVolumeId *NfsVolumeId) GetServiceName() string { return nfsVolumeId.Service }
func (nfsVolumeId *NfsVolumeId) SetServiceName(name string) { nfsVolumeId.Service = name }
func (nfsVolumeId *NfsVolumeId) GetObjectPath() string {
	return fmt.Sprintf("%s/%s/%s", nfsVolumeId.Cluster, nfsVolumeId.Tenant, nfsVolumeId.Bucket)
}
func (nfsVolumeId *NfsVolumeId) Validate() error {
	missed := make([]string, 0)
	if nfsVolumeId.Cluster == "" {
		missed = append(missed, "Cluster")
	}
	if nfsVolumeId.Tenant == "" {
		missed = append(missed, "Tenant")
	}
	if nfsVolumeId.Bucket == "" {
		missed = append(missed, "Bucket")
	}
	if len(missed) > 0 {
		return fmt.Errorf("Those parameter(s) missed %s", strings.Join(missed, ","))
	}
	return nil
}

type IscsiVolumeId struct {
	Service string
	Cluster string
	Tenant  string
	Bucket  string
	Object  string
}

func (iscsiVolumeId *IscsiVolumeId) GetServiceName() string { return iscsiVolumeId.Service }
func (iscsiVolumeId *IscsiVolumeId) SetServiceName(name string) { iscsiVolumeId.Service = name }
func (iscsiVolumeId *IscsiVolumeId) GetObjectPath() string {
	return fmt.Sprintf("%s/%s/%s/%s", iscsiVolumeId.Cluster, iscsiVolumeId.Tenant, iscsiVolumeId.Bucket, iscsiVolumeId.Object)
}

func (iscsiVolumeId *IscsiVolumeId) Validate() error {
	missed := make([]string, 0)
	if iscsiVolumeId.Cluster == "" {
		missed = append(missed, "Cluster")
	}
	if iscsiVolumeId.Tenant == "" {
		missed = append(missed, "Tenant")
	}
	if iscsiVolumeId.Bucket == "" {
		missed = append(missed, "Bucket")
	}
	if iscsiVolumeId.Bucket == "" {
		missed = append(missed, "Object")
	}

	if len(missed) > 0 {
		return fmt.Errorf("Those parameter(s) are missed %s", strings.Join(missed, ","))
	}
	return nil
}

func ParseNfsVolumeID(volumeID string, configOptions map[string]string) (vol *NfsVolumeId, err error) {

	vol = &NfsVolumeId{}

	parts := strings.Split(volumeID, "@")

	// object path elements like cluster/tenant/bucket
	var pathObjects []string
	if len(parts) < 2 {
		// no service notation
		if service, ok := configOptions["service"]; ok {
			vol.Service = service
		}
		pathObjects = strings.Split(parts[0], "/")
	} else {
		vol.Service = parts[0]
		if vol.Service == "" {
			if service, ok := configOptions["service"]; ok {
				vol.Service = service
			}
		}
		pathObjects = strings.Split(parts[1], "/")
	}

	// bucket only
	if len(pathObjects) == 1 {
		if cluster, ok := configOptions["cluster"]; ok {
			vol.Cluster = cluster
		}

		if tenant, ok := configOptions["tenant"]; ok {
			vol.Tenant = tenant
		}

		vol.Bucket = pathObjects[0]
	} else if len(pathObjects) == 2 {
		// tenant and bucket only

		if cluster, ok := configOptions["cluster"]; ok {
			vol.Cluster = cluster
		}

		vol.Tenant = pathObjects[0]
		if vol.Tenant == "" {
			if tenant, ok := configOptions["tenant"]; ok {
				vol.Tenant = tenant
			}
		}

		vol.Bucket = pathObjects[1]
	} else {
		// cluster, tenant and bucket

		//Cluster
		vol.Cluster = pathObjects[0]
		if vol.Cluster == "" {
			if cluster, ok := configOptions["cluster"]; ok {
				vol.Cluster = cluster
			}
		}

		//Tenant
		vol.Tenant = pathObjects[1]
		if vol.Tenant == "" {
			if tenant, ok := configOptions["tenant"]; ok {
				vol.Tenant = tenant
			}
		}

		//Bucket
		vol.Bucket = pathObjects[2]
	}

	return vol, vol.Validate()
}

func ParseIscsiVolumeID(volumeID string, configOptions map[string]string) (vol *IscsiVolumeId, err error) {

	vol = &IscsiVolumeId{}

	parts := strings.Split(volumeID, "@")

	// object path elements like cluster/tenant/bucket
	var pathObjects []string
	if len(parts) < 2 {
		// no service notation
		if service, ok := configOptions["service"]; ok {
			vol.Service = service
		}
		pathObjects = strings.Split(parts[0], "/")
	} else {
		vol.Service = parts[0]
		if vol.Service == "" {
			if service, ok := configOptions["service"]; ok {
				vol.Service = service
			}
		}
		pathObjects = strings.Split(parts[1], "/")
	}

	// lun only
	if len(pathObjects) == 1 {
		if cluster, ok := configOptions["cluster"]; ok {
			vol.Cluster = cluster
		}

		if tenant, ok := configOptions["tenant"]; ok {
			vol.Tenant = tenant
		}

		if bucket, ok := configOptions["bucket"]; ok {
			vol.Bucket = bucket
		}

		vol.Object = pathObjects[0]
	} else if len(pathObjects) == 2 {
		// bucket, lun
		if cluster, ok := configOptions["cluster"]; ok {
			vol.Cluster = cluster
		}

		if tenant, ok := configOptions["tenant"]; ok {
			vol.Tenant = tenant
		}

		vol.Bucket = pathObjects[0]
		if vol.Bucket == "" {
			if bucket, ok := configOptions["bucket"]; ok {
				vol.Bucket = bucket
			}
		}

		vol.Object = pathObjects[1]
	} else if len(pathObjects) == 3 {
		// tenant, bucket, lun

		if cluster, ok := configOptions["cluster"]; ok {
			vol.Cluster = cluster
		}

		vol.Tenant = pathObjects[0]
		if vol.Tenant == "" {
			if tenant, ok := configOptions["tenant"]; ok {
				vol.Tenant = tenant
			}
		}

		vol.Bucket = pathObjects[1]
		if vol.Bucket == "" {
			if bucket, ok := configOptions["bucket"]; ok {
				vol.Bucket = bucket
			}
		}

		vol.Object = pathObjects[2]
	} else {
		// cluster, tenant, bucket, lun

		//Cluster
		vol.Cluster = pathObjects[0]
		if vol.Cluster == "" {
			if cluster, ok := configOptions["cluster"]; ok {
				vol.Cluster = cluster
			}
		}

		//Tenant
		vol.Tenant = pathObjects[1]
		if vol.Tenant == "" {
			if tenant, ok := configOptions["tenant"]; ok {
				vol.Tenant = tenant
			}
		}

		//Bucket
		vol.Bucket = pathObjects[2]
		if vol.Bucket == "" {
			if bucket, ok := configOptions["bucket"]; ok {
				vol.Bucket = bucket
			}
		}
		vol.Object = pathObjects[3]
	}

	return vol, vol.Validate()
}
