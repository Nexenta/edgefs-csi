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
	"encoding/json"
	"errors"
	"fmt"
	"time"
	"strings"
	"../../../grpc-efsproxy/cluster"
	"../../../grpc-efsproxy/tenant"
//	"../../../grpc-efsproxy/service"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	log "github.com/sirupsen/logrus"
)

const (
	defaultSize      int = 1024
)

var (
	defaultTimeout   time.Duration = 10 * time.Second
)

type EdgefsNFSVolume struct {
	VolumeID VolumeID
	Path     string
	Share    string
}

type EdgefsService struct {
	Name        string
	ServiceType string
	Status      string
	Network     []string
}

func (edgefsService *EdgefsService) FindNFSVolumeByVolumeID(volumeID string, nfsVolumes []EdgefsNFSVolume) (resultNfsVolume EdgefsNFSVolume, err error) {

	for _, nfsVolume := range nfsVolumes {
		if nfsVolume.VolumeID.String() == volumeID {
			return nfsVolume, nil
		}
	}
	return resultNfsVolume, errors.New("Can't find NFS volume by volumeID :" + volumeID)
}

func (edgefsService *EdgefsService) GetNFSVolumeAndEndpoint(volumeID string, service EdgefsService, nfsVolumes []EdgefsNFSVolume) (nfsVolume EdgefsNFSVolume, endpoint string, err error) {
	nfsVolume, err = edgefsService.FindNFSVolumeByVolumeID(volumeID, nfsVolumes)
	if err != nil {
		return nfsVolume, "", err
	}

	return nfsVolume, fmt.Sprintf("%s:%s", service.Network[0], nfsVolume.Share), err
}

/*IEdgeFS interface to provide base methods */
type IEdgeFSProvider interface {
	ListClusters() (clusters []string, err error)
	ListTenants(cluster string) (tenants []string, err error)
	ListBuckets(cluster string, tenant string) (buckets []string, err error)
	IsBucketExist(cluster string, tenant string, bucket string) bool
	CreateBucket(cluster string, tenant string, bucket string, size int, options map[string]string) error
	DeleteBucket(cluster string, tenant string, bucket string, force bool) error
	ServeBucket(service string, cluster string, tenant string, bucket string) (err error)
	UnserveBucket(service string, cluster string, tenant string, bucket string) (err error)
	SetBucketQuota(cluster string, tenant string, bucket string, quota string) (err error)
	SetServiceAclConfiguration(service string, tenant string, bucket string, value string) error
	UnsetServiceAclConfiguration(service string, tenant string, bucket string) error
	ListServices() (services []EdgefsService, err error)
	GetService(serviceName string) (service EdgefsService, err error)
	ListNFSVolumes(serviceName string) (nfsVolumes []EdgefsNFSVolume, err error)
	CheckHealth() (err error)
}

type EdgeFSProvider struct {
	endpoint   string
	auth       string
	conn       *grpc.ClientConn
}

type loginCreds struct {
	Username, Password string
}

func (c *loginCreds) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{
		"username": c.Username,
		"password": c.Password,
	}, nil
}

func (c *loginCreds) RequireTransportSecurity() bool {
	return true
}

func InitEdgeFSProvider(proxyip string, port int16, username string, password string) IEdgeFSProvider {
	log.SetLevel(log.DebugLevel)

	nexentaEdgeProviderInstance := &EdgeFSProvider{
		endpoint:   fmt.Sprintf("%s:%d", proxyip, port),
	}

	conn, err := grpc.Dial(nexentaEdgeProviderInstance.endpoint,
	    grpc.WithInsecure(),
	    grpc.WithPerRPCCredentials(&loginCreds{
		Username: username,
		Password: password,
	}))
	if err != nil {
		log.Printf("did not connect: %v", err)
		return nil
	}

	nexentaEdgeProviderInstance.conn = conn

	return nexentaEdgeProviderInstance
}

func (edgefs *EdgeFSProvider) CheckHealth() (err error) {
	c := cluster.NewClusterClient(edgefs.conn)
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	r, err := c.CheckHealth(ctx, &cluster.CheckHealthRequest{})
	if err != nil {
		err = fmt.Errorf("CheckHealth: cannot contact local site cluster", err)
		log.Error(err.Error)
		return err
	}
	if r.Status != "ok" {
		err = fmt.Errorf("wrong response of the CheckHealth call: expecting ok got %s", r.Status)
		log.Error(err.Error)
		return err
	}

	return nil
}

func (edgefs *EdgeFSProvider) CreateBucket(clusterName string, tenantName string, bucketName string, size int, options map[string]string) (err error) {
	// TODO: options
	c := tenant.NewBucketClient(edgefs.conn)
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	_, err = c.BucketCreate(ctx, &tenant.BucketCreateRequest{})
	if err != nil {
		err = fmt.Errorf("BucketCreate:", err)
		log.Error(err.Error)
		return err
	}

	return nil
}

func (edgefs *EdgeFSProvider) DeleteBucket(clusterName string, tenantName string, bucketName string, force bool) (err error) {

	if force == true {
		// TODO: expunge=1, async=1

		c := tenant.NewBucketClient(edgefs.conn)
		ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
		defer cancel()

		_, err = c.BucketDelete(ctx, &tenant.BucketDeleteRequest{})
		if err != nil {
			err = fmt.Errorf("BucketDelete:", err)
			log.Error(err.Error)
			return err
		}
	}

	return nil
}

func (edgefs *EdgeFSProvider) SetServiceAclConfiguration(service string, tenant string, bucket string, value string) error {
	// TODO: implement
	return nil
}

func (edgefs *EdgeFSProvider) UnsetServiceAclConfiguration(service string, tenant string, bucket string) error {
	// TODO: implement
	return nil
}

func (edgefs *EdgeFSProvider) SetBucketQuota(cluster string, tenant string, bucket string, quota string) (err error) {
	// TODO: implement
	return err
}

func (edgefs *EdgeFSProvider) GetService(serviceName string) (service EdgefsService, err error) {
	// TODO: implement
	return service, err
}

func (edgefs *EdgeFSProvider) ListServices() (services []EdgefsService, err error) {
	// TODO: implement
	return services, err
}

func (edgefs *EdgeFSProvider) ListNFSVolumes(serviceName string) (nfsVolumes []EdgefsNFSVolume, err error) {
	// TODO: implement
	return nfsVolumes, err
}

func (edgefs *EdgeFSProvider) ServeBucket(service string, cluster string, tenant string, bucket string) (err error) {
	// TODO: implement
	return nil
}

func (edgefs *EdgeFSProvider) UnserveBucket(service string, cluster string, tenant string, bucket string) (err error) {
	// TODO: implement
	return nil
}

func (edgefs *EdgeFSProvider) IsBucketExist(cluster string, tenant string, bucket string) bool {
	// TODO: implement
	return true
}

func (edgefs *EdgeFSProvider) ListBuckets(cluster string, tenant string) (buckets []string, err error) {
	// TODO: implement
	return buckets, err
}

func (edgefs *EdgeFSProvider) ListClusters() (clusters []string, err error) {
	// TODO: implement
	return clusters, err
}

func (edgefs *EdgeFSProvider) ListTenants(cluster string) (tenants []string, err error) {
	// TODO: implement
	return tenants, err
}

func getXServiceObjectsFromString(service string, xObjects string) (nfsVolumes []EdgefsNFSVolume, err error) {
	var objects []string
	err = json.Unmarshal([]byte(xObjects), &objects)
	if err != nil {
		log.Error(err)
		return nfsVolumes, err
	}

	// Service Object string format: <id>,<ten/buc>@<clu/ten/buc>
	for _, v := range objects {
		var objectParts = strings.Split(v, ",")
		if len(objectParts) == 2 {

			parts := strings.Split(objectParts[1], "@")
			if len(parts) == 2 {
				pathParts := strings.Split(parts[1], "/")
				if len(pathParts) == 3 {
					share := "/" + parts[0]
					volume := EdgefsNFSVolume{
						VolumeID: VolumeID{
							Service: service, Cluster: pathParts[0],
							Tenant: pathParts[1], Bucket: pathParts[2],
						},
						Share: share,
						Path:  parts[1],
					}
					nfsVolumes = append(nfsVolumes, volume)
				}
			}
		}
	}
	return nfsVolumes, err
}
