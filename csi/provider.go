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
	"errors"
	"fmt"
	"time"
	"strings"
	"../../../grpc-efsproxy/cluster"
	"../../../grpc-efsproxy/tenant"
	"../../../grpc-efsproxy/service"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	log "github.com/sirupsen/logrus"
)

const (
	defaultSize		int = 1024
	defaultEfsProxyPort	int32 = 6789
)

var (
	defaultTimeout   time.Duration = 20 * time.Second
)

type EdgefsNFSVolume struct {
	VolumeID VolumeID
	Path     string
	Share    string
}

type EdgefsService struct {
	Name         string
	K8SSvcName   string
	K8SNamespace string
	ServiceType  string
	Network      []string
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
	ServeBucket(service, k8sservice, k8snamespace string, cluster string, tenant string, bucket string) (err error)
	UnserveBucket(service, k8sservice, k8snamespace string, cluster string, tenant string, bucket string) (err error)
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
	return false
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
		err = fmt.Errorf("CheckHealth: cannot contact local site cluster: %v", err)
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

	_, err = c.BucketCreate(ctx, &tenant.BucketCreateRequest{Cluster: clusterName, Tenant: tenantName, Bucket: bucketName})
	if err != nil {
		err = fmt.Errorf("BucketCreate: %v", err)
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

		_, err = c.BucketDelete(ctx, &tenant.BucketDeleteRequest{Cluster: clusterName, Tenant: tenantName, Bucket: bucketName})
		if err != nil {
			err = fmt.Errorf("BucketDelete: %v", err)
			log.Error(err.Error)
			return err
		}
	}

	return nil
}

func (edgefs *EdgeFSProvider) SetServiceAclConfiguration(service string, tenant string, bucket string, value string) error {
	// TODO: implement
	//c := tenant.NewBucketClient(edgefs.conn)
	//ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	//defer cancel()


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

func (edgefs *EdgeFSProvider) GetService(serviceName string) (serviceOut EdgefsService, err error) {
	c := service.NewServiceClient(edgefs.conn)
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	r, err := c.ServiceList(ctx, &service.ServiceListRequest{
		Type: service.ProtocolType_NFS,
		Pattern: serviceName,
		Count: 1,
	})
	if err != nil {
		err = fmt.Errorf("ServiceList Pattern=%s Count=1: %v", serviceName, err)
		log.Error(err.Error)
		return EdgefsService{}, err
	}

	for _,info := range r.Info {
		serviceOut.Name = info.Name
		serviceOut.ServiceType = info.Type.String()
		serviceOut.Network = nil // will be filled in by detector
		break
	}
	return serviceOut, nil
}

func (edgefs *EdgeFSProvider) ListServices() (services []EdgefsService, err error) {
	c := service.NewServiceClient(edgefs.conn)
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	r, err := c.ServiceList(ctx, &service.ServiceListRequest{
		Type: service.ProtocolType_NFS,
	})
	if err != nil {
		err = fmt.Errorf("ServiceList: %v", err)
		log.Error(err.Error)
		return nil, err
	}

	for _,info := range r.Info {
		var service EdgefsService
		service.Name = info.Name
		service.ServiceType = info.Type.String()
		service.Network = nil // will be filled in by detector
		services = append(services, service)
	}
	return services, nil
}

func (edgefs *EdgeFSProvider) ListNFSVolumes(serviceName string) (nfsVolumes []EdgefsNFSVolume, err error) {
	c := service.NewServiceClient(edgefs.conn)
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	r, err := c.ServiceObjectList(ctx, &service.ServiceObjectListRequest{
		Service: serviceName,
		Count: 1,
	})
	if err != nil {
		err = fmt.Errorf("ServiceObjectList: %v", err)
		log.Error(err.Error)
		return nil, err
	}

	for _,info := range r.Info {
		var objectParts = strings.Split(info.Name, ",")
		if len(objectParts) == 2 {
			parts := strings.Split(objectParts[1], "@")
			if len(parts) == 2 {
				pathParts := strings.Split(parts[1], "/")
				if len(pathParts) == 3 {
					share := "/" + parts[0]
					volume := EdgefsNFSVolume{
						VolumeID: VolumeID{
							Service: serviceName,
							Cluster: pathParts[0],
							Tenant: pathParts[1],
							Bucket: pathParts[2],
						},
						Share: share,
						Path:  parts[1],
					}
					nfsVolumes = append(nfsVolumes, volume)
				}
			}
		}
	}
	return nfsVolumes, nil
}

func (edgefs *EdgeFSProvider) ServeBucket(serviceName, k8sServiceName, k8sNamespace string,
		clusterName string, tenantName string, bucketName string) (err error) {
	c := service.NewServiceClient(edgefs.conn)
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	req := &service.ServeRequest{
		Type: service.ProtocolType_NFS,
		Service: serviceName,
		K8SService: k8sServiceName,	// Kubernetes EdgeNFS Service
		K8SNamespace: k8sNamespace,	// - namespace to search for service
		Cluster: clusterName,
		Tenant: tenantName,
		Bucket: bucketName,
	}

	log.Printf("ServeBucket: %+v", req)

	_, err = c.Serve(ctx, req)
	if err != nil {
		err = fmt.Errorf("Serve: %v", err)
		log.Error(err.Error)
		return err
	}
	return nil
}

func (edgefs *EdgeFSProvider) UnserveBucket(serviceName, k8sServiceName, k8sNamespace string,
		clusterName string, tenantName string, bucketName string) (err error) {
	c := service.NewServiceClient(edgefs.conn)
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	req := &service.ServeRequest{
		Type: service.ProtocolType_NFS,
		Service: serviceName,
		K8SService: k8sServiceName,	// Kubernetes EdgeNFS Service
		K8SNamespace: k8sNamespace,	// - namespace to search for service
		Cluster: clusterName,
		Tenant: tenantName,
		Bucket: bucketName,
	}
	log.Printf("UnserveBucket: %+v", req)

	_, err = c.Unserve(ctx, req)
	if err != nil {
		err = fmt.Errorf("Unserve: %v", err)
		log.Error(err.Error)
		return err
	}
	return nil
}

func (edgefs *EdgeFSProvider) IsBucketExist(clusterName string, tenantName string, bucketName string) bool {
	c := tenant.NewBucketClient(edgefs.conn)
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	r, err := c.BucketList(ctx, &tenant.BucketListRequest{
		Cluster: clusterName,
		Tenant: tenantName,
		Pattern: bucketName,
		Count: 1,
	})
	if err != nil {
		err = fmt.Errorf("BucketList Pattern=%s Count=1: %v", bucketName, err)
		log.Error(err.Error)
		return false
	}

	return len(r.Info) == 1
}

func (edgefs *EdgeFSProvider) ListBuckets(clusterName string, tenantName string) (buckets []string, err error) {
	c := tenant.NewBucketClient(edgefs.conn)
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	r, err := c.BucketList(ctx, &tenant.BucketListRequest{
		Cluster: clusterName,
		Tenant: tenantName,
	})
	if err != nil {
		err = fmt.Errorf("BucketList: %v", err)
		log.Error(err.Error)
		return nil, err
	}

	for _,info := range r.Info {
		buckets = append(buckets, info.Name)
	}
	return buckets, err
}

func (edgefs *EdgeFSProvider) ListClusters() (clusters []string, err error) {
	c := cluster.NewClusterClient(edgefs.conn)
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	r, err := c.ClusterList(ctx, &cluster.ClusterListRequest{})
	if err != nil {
		err = fmt.Errorf("ClusterList: %v", err)
		log.Error(err.Error)
		return nil, err
	}

	for _,info := range r.Info {
		clusters = append(clusters, info.Name)
	}

	return clusters, nil
}

func (edgefs *EdgeFSProvider) ListTenants(clusterName string) (tenants []string, err error) {
	c := tenant.NewTenantClient(edgefs.conn)
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	r, err := c.TenantList(ctx, &tenant.TenantListRequest{
		Cluster: clusterName,
	})
	if err != nil {
		err = fmt.Errorf("TenantList: %v", err)
		log.Error(err.Error)
		return nil, err
	}

	for _,info := range r.Info {
		tenants = append(tenants, info.Name)
	}
	return tenants, err
}
