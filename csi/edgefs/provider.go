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
	//	"errors"
	"fmt" //"strings"
	"time"

	"../../../../grpc-efsproxy/cluster"
	"../../../../grpc-efsproxy/service"
	"../../../../grpc-efsproxy/snapshot"
	"../../../../grpc-efsproxy/tenant"
	logrus "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

const (
	defaultSize         int   = 1024
	defaultEfsProxyPort int32 = 6789
)

var (
	defaultTimeout time.Duration = 20 * time.Second
)

/*IEdgeFS interface to provide base methods */
type IEdgeFSProvider interface {
	// common provider methods
	ListClusters() (clusters []string, err error)
	ListTenants(cluster string) (tenants []string, err error)
	ListBuckets(cluster string, tenant string) (buckets []string, err error)
	//ListObjects(cluster, tenant, bucket string) ([]string, error)
	IsBucketExist(cluster string, tenant string, bucket string) bool

	//TODO:
	//IsObjectExist(cluster, tenant, bucket, object string) bool
	//DeleteObject(cluster, tenant, bucket, object string) error

	CreateBucket(cluster string, tenant string, bucket string, size int, options map[string]string) error
	DeleteBucket(cluster string, tenant string, bucket string, force bool) error

	//ServeObject serve object to NFS/ISCSI service, returns ISCSI object path lunNumber@cluster/tenant/bucket/object
	ServeObject(serviceName, k8sservice, k8snamespace string, volume EdgefsIscsiVolume, settings VolumeSettings) (objectPath string, err error)
	UnserveObject(serviceName, k8sservice, k8snamespace string, volume EdgefsIscsiVolume) (err error)

	//ServeBucket serve bucket to NFS service
	ServeBucket(service, k8sservice, k8snamespace string, volume EdgefsNfsVolume, settings VolumeSettings) (objectPath string, err error)
	UnserveBucket(service, k8sservice, k8snamespace string, volume EdgefsNfsVolume) (err error)

	SetBucketQuota(cluster string, tenant string, bucket string, quota string) (err error)
	SetServiceAclConfiguration(service string, tenant string, bucket string, value string) error
	UnsetServiceAclConfiguration(service string, tenant string, bucket string) error

	/* multi service methods */
	GetService(serviceType, serviceName string) (service IEdgefsService, err error)
	//ListServices(serviceType string) ([]IEdgefsService, error)
	ListVolumes(serviceType, serviceName string) (volumes []IEdgefsVolume, err error)

	//CloneIscsiVolume
	CloneVolumeFromSnapshot(cloneVolume IscsiVolumeId, ss IscsiSnapshotId) (CloneInfo, error)

	//Snapshot methods
	IsSnapshotExists(ss IscsiSnapshotId) (bool, error)
	CreateSnapshot(ss IscsiSnapshotId) (SnapshotInfo, error)
	DeleteSnapshot(ss IscsiSnapshotId) error
	ListSnapshots(volume IscsiVolumeId, pattern string) ([]SnapshotInfo, error)

	CheckHealth() (err error)
}

type EdgeFSProvider struct {
	endpoint string
	auth     string
	conn     *grpc.ClientConn
	logger   *logrus.Entry
}

type loginCreds struct {
	Username, Password string
}

type VolumeSettings struct {
	IsClonedObject bool
	VolumeSize     int64
	ChunkSize      int32
	BlockSize      int32
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

func InitEdgeFSProvider(proxyip string, port int16, username string, password string, logger *logrus.Entry) IEdgeFSProvider {
	l := logger.WithField("cmp", "provider")

	nexentaEdgeProviderInstance := &EdgeFSProvider{
		endpoint: fmt.Sprintf("%s:%d", proxyip, port),
		logger:   l,
	}

	conn, err := grpc.Dial(nexentaEdgeProviderInstance.endpoint,
		grpc.WithInsecure(),
		grpc.WithPerRPCCredentials(&loginCreds{
			Username: username,
			Password: password,
		}))
	if err != nil {
		l.Errorf("did not connect: %v", err)
		return nil
	}

	nexentaEdgeProviderInstance.conn = conn

	return nexentaEdgeProviderInstance
}

func (provider *EdgeFSProvider) CheckHealth() (err error) {
	l := provider.logger.WithField("func", "CheckHealth()")
	c := cluster.NewClusterClient(provider.conn)
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	response, err := c.CheckHealth(ctx, &cluster.CheckHealthRequest{})
	if err != nil {
		l.Error(err.Error)
		return err
	}
	l.Infof("response: '%+v'", *response)
	if response.Status != "ok" {
		l.Error(err.Error)
		return err
	}

	return nil
}

func (provider *EdgeFSProvider) CreateBucket(clusterName string, tenantName string, bucketName string, size int, options map[string]string) (err error) {
	// TODO: options
	l := provider.logger.WithField("func", "CreateBucket()")
	c := tenant.NewBucketClient(provider.conn)
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	request := &tenant.BucketCreateRequest{Cluster: clusterName, Tenant: tenantName, Bucket: bucketName}
	l.Infof("request: '%+v'", *request)
	response, err := c.BucketCreate(ctx, request)
	if err != nil {
		err = fmt.Errorf("BucketCreate: %v", err)
		l.Error(err.Error)
		return err
	}
	l.Infof("response: '%+v'", *response)
	return nil
}

func (provider *EdgeFSProvider) DeleteBucket(clusterName string, tenantName string, bucketName string, force bool) (err error) {
	l := provider.logger.WithField("func", "DeleteBucket()")
	request := &tenant.BucketDeleteRequest{Cluster: clusterName, Tenant: tenantName, Bucket: bucketName}
	l.Infof("request: '%+v'", *request)
	l.Infof("force: '%t'", force)

	if force == true {
		// TODO: expunge=1, async=1

		c := tenant.NewBucketClient(provider.conn)
		ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
		defer cancel()

		_, err = c.BucketDelete(ctx, request)
		if err != nil {
			l.Error(err.Error)
			return err
		}
	}

	return nil
}

func (provider *EdgeFSProvider) SetServiceAclConfiguration(service string, tenant string, bucket string, value string) error {
	// TODO: implement
	provider.logger.WithField("func", "SetServiceAclConfiguration()")
	return nil
}

func (provider *EdgeFSProvider) UnsetServiceAclConfiguration(service string, tenant string, bucket string) error {
	// TODO: implement
	provider.logger.WithField("func", "UnsetServiceAclConfiguration()")
	return nil
}

func (provider *EdgeFSProvider) SetBucketQuota(cluster string, tenant string, bucket string, quota string) (err error) {
	// TODO: implement
	provider.logger.WithField("func", "SetBucketQuota()")
	return err
}

func (provider *EdgeFSProvider) getProtocolType(serviceType string) (service.ProtocolType, error) {
	switch serviceType {
	case EdgefsServiceType_NFS:
		return service.ProtocolType_NFS, nil
	case EdgefsServiceType_ISCSI:
		return service.ProtocolType_ISCSI, nil
	}
	return service.ProtocolType_UNKNOWN, fmt.Errorf("Unknown service type %s ", serviceType)
}

func (provider *EdgeFSProvider) GetService(serviceType string, serviceName string) (serviceOut IEdgefsService, err error) {
	l := provider.logger.WithField("func", "GetService()")
	c := service.NewServiceClient(provider.conn)
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	protocolType, err := provider.getProtocolType(serviceType)
	if err != nil {
		return nil, err
	}

	request := &service.ServiceListRequest{
		Type:    protocolType,
		Pattern: serviceName,
		Count:   1,
	}
	l.Infof("request: '%+v'", *request)
	response, err := c.ServiceList(ctx, request)
	if err != nil {
		err = fmt.Errorf("ServiceList Pattern=%s Count=1: %v", serviceName, err)
		l.Error(err.Error)
		return nil, err
	}

	l.Infof("response: '%+v'", response)

	for _, info := range response.Info {
		switch serviceType {
		case EdgefsServiceType_NFS:
			serviceOut = &EdgefsNfsService{EdgefsService: EdgefsService{Name: info.Name, Type: EdgefsServiceType_NFS}}
		case EdgefsServiceType_ISCSI:
			serviceOut = &EdgefsIscsiService{EdgefsService: EdgefsService{Name: info.Name, Type: EdgefsServiceType_ISCSI}, TargetIqn: info.Iqn}
		default:
			l.Errorf("Unknown serviceType: %s", serviceType)
		}
	}
	return serviceOut, nil
}

/*
func (edgefs *EdgeFSProvider) ListServices(backendType string) (services []IEdgefsService, err error) {
	c := service.NewServiceClient(edgefs.conn)
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	protocolType, err := edgefs.getProtocolType(backendType)
	if err != nil {
	        return nil, err
        }

	r, err := c.ServiceList(ctx, &service.ServiceListRequest{
		Type: protocolType,
	})

	if err != nil {
		err = fmt.Errorf("ServiceList: %v", err)
		log.Error(err.Error)
		return nil, err
	}

	for _,info := range r.Info {
		var service IEdgefsService
		service, err = edgefs.NewEdgefsService(backendType, info)
                if err != nil {
                        return nil, err
                }
		services = append(services, service)
	}
	return services, nil
}
*/
func (provider *EdgeFSProvider) ListVolumes(serviceType, serviceName string) ([]IEdgefsVolume, error) {
	l := provider.logger.WithField("func", "ListVolumes()")
	c := service.NewServiceClient(provider.conn)
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	request := &service.ServiceObjectListRequest{
		Service: serviceName,
	}
	l.Infof("request: '%+v'", *request)

	response, err := c.ServiceObjectList(ctx, request)
	if err != nil {
		err = fmt.Errorf("Error: %s", err)
		l.Error(err.Error)
		return nil, err
	}
	l.Infof("response: '%+v'", response)
	volumes := make([]IEdgefsVolume, 0)
	for _, info := range response.Info {

		var volume IEdgefsVolume
		switch serviceType {
		case EdgefsServiceType_NFS:
			volume, err = ParseEdgefsNfsVolumeStr(info.Name)
		case EdgefsServiceType_ISCSI:
			volume, err = ParseEdgefsIscsiVolumeStr(info.Name)
		default:
			return nil, fmt.Errorf("Unknown service type %s ", serviceType)
		}

		if err != nil {
			return nil, err
		}
		volumes = append(volumes, volume)
	}
	return volumes, nil
}

func (provider *EdgeFSProvider) ServeBucket(serviceName, k8sServiceName, k8sNamespace string,
	volume EdgefsNfsVolume, settings VolumeSettings) (objectPath string, rr error) {
	l := provider.logger.WithField("func", "ServeBucket()")
	c := service.NewServiceClient(provider.conn)
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	request := &service.ServeRequest{
		Type:         service.ProtocolType_NFS,
		Service:      serviceName,
		K8SService:   k8sServiceName, // Kubernetes EdgeNFS Service
		K8SNamespace: k8sNamespace,   // - namespace to search for service
		Cluster:      volume.Cluster,
		Tenant:       volume.Tenant,
		Bucket:       volume.Bucket,
		VolumeSettings: &service.VolumeSettings{
			VolumeSize: settings.VolumeSize,
			ChunkSize:  settings.ChunkSize,
			BlockSize:  settings.BlockSize,
		},
	}
	l.Infof("request: '%+v'", *request)

	response, err := c.Serve(ctx, request)
	if err != nil {
		err = fmt.Errorf("Serve: %v", err)
		l.Error(err.Error)
		return "", err
	}
	l.Infof("response: '%+v'", response)
	return response.VolumePath, nil
}

//ServeBucket serve object to  ISCSI service, returns ISCSI object path lunNumber@cluster/tenant/bucket/lun
func (provider *EdgeFSProvider) ServeObject(serviceName, k8sServiceName, k8sNamespace string, volume EdgefsIscsiVolume, settings VolumeSettings) (objectPath string, err error) {
	l := provider.logger.WithField("func", "ServeObject()")
	c := service.NewServiceClient(provider.conn)
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	request := &service.ServeRequest{
		Type:         service.ProtocolType_ISCSI,
		Service:      serviceName,
		K8SService:   k8sServiceName, // Kubernetes EdgeNFS Service
		K8SNamespace: k8sNamespace,   // - namespace to search for service
		Cluster:      volume.Cluster,
		Tenant:       volume.Tenant,
		Bucket:       volume.Bucket,
		Object:       volume.Object,
		VolumeSettings: &service.VolumeSettings{
			IsClonedObject: settings.IsClonedObject,
			VolumeSize: settings.VolumeSize,
			ChunkSize:  settings.ChunkSize,
			BlockSize:  settings.BlockSize,
		},
	}
	l.Infof("request: '%+v'", *request)

	response, err := c.Serve(ctx, request)
	if err != nil {
		err = fmt.Errorf("Serve: %v", err)
		l.Error(err.Error)
		return "", err
	}
	l.Infof("response: '%+v'", response)
	return response.VolumePath, nil
}

func (provider *EdgeFSProvider) UnserveObject(serviceName, k8sService, k8sNamespace string, volume EdgefsIscsiVolume) (err error) {
	l := provider.logger.WithField("func", "UnserveObject()")
	c := service.NewServiceClient(provider.conn)
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	request := &service.ServeRequest{
		Type:         service.ProtocolType_ISCSI,
		Service:      serviceName,
		K8SService:   k8sService,   // Kubernetes EdgeNFS Service
		K8SNamespace: k8sNamespace, // - namespace to search for service
		Cluster:      volume.Cluster,
		Tenant:       volume.Tenant,
		Bucket:       volume.Bucket,
		Object:       volume.Object,
		VolumeId:     volume.LunNumber,
	}
	l.Infof("request: '%+v'", *request)

	response, err := c.Unserve(ctx, request)
	if err != nil {
		l.Error(err.Error)
		return err
	}
	l.Infof("response: '%+v'", response)
	return nil
}

func (provider *EdgeFSProvider) UnserveBucket(serviceName, k8sServiceName, k8sNamespace string,
	volume EdgefsNfsVolume) (err error) {
	l := provider.logger.WithField("func", "UnserveBucket()")
	c := service.NewServiceClient(provider.conn)
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	request := &service.ServeRequest{
		Type:         service.ProtocolType_NFS,
		Service:      serviceName,
		K8SService:   k8sServiceName, // Kubernetes EdgeNFS Service
		K8SNamespace: k8sNamespace,   // - namespace to search for service
		Cluster:      volume.Cluster,
		Tenant:       volume.Tenant,
		Bucket:       volume.Bucket,
	}
	l.Infof("request: '%+v'", *request)

	response, err := c.Unserve(ctx, request)
	if err != nil {
		l.Error(err.Error)
		return err
	}
	l.Infof("response: '%+v'", response)
	return nil
}

func (provider *EdgeFSProvider) IsBucketExist(clusterName string, tenantName string, bucketName string) bool {
	l := provider.logger.WithField("func", "IsBucketExist()")
	c := tenant.NewBucketClient(provider.conn)
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	request := &tenant.BucketListRequest{
		Cluster: clusterName,
		Tenant:  tenantName,
		Pattern: bucketName,
		Count:   1,
	}
	l.Infof("request: '%+v'", *request)

	response, err := c.BucketList(ctx, request)
	if err != nil {
		err = fmt.Errorf("BucketList Pattern=%s Count=1: %v", bucketName, err)
		l.Error(err.Error)
		return false
	}
	l.Infof("response: '%+v'", response)
	return len(response.Info) == 1
}

func (provider *EdgeFSProvider) ListBuckets(clusterName string, tenantName string) (buckets []string, err error) {
	l := provider.logger.WithField("func", "ListBuckets()")
	c := tenant.NewBucketClient(provider.conn)
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	request := &tenant.BucketListRequest{
		Cluster: clusterName,
		Tenant:  tenantName,
	}
	l.Infof("request: '%+v'", *request)

	response, err := c.BucketList(ctx, request)
	if err != nil {
		l.Error(err.Error)
		return nil, err
	}
	l.Infof("response: '%+v'", response)
	for _, info := range response.Info {
		buckets = append(buckets, info.Name)
	}
	l.Infof("Buckets: '%+v'", buckets)
	return buckets, err
}

func (provider *EdgeFSProvider) ListClusters() (clusters []string, err error) {
	l := provider.logger.WithField("func", "ListClusters()")
	c := cluster.NewClusterClient(provider.conn)
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	request := &cluster.ClusterListRequest{}
	l.Infof("request: '%+v'", *request)
	response, err := c.ClusterList(ctx, request)
	if err != nil {
		l.Error(err.Error)
		return nil, err
	}
	l.Infof("response: '%+v'", response)

	for _, info := range response.Info {
		clusters = append(clusters, info.Name)
	}
	l.Infof("Clusters: '%+v'", clusters)

	return clusters, nil
}

func (provider *EdgeFSProvider) ListTenants(clusterName string) (tenants []string, err error) {
	l := provider.logger.WithField("func", "ListTenants()")
	c := tenant.NewTenantClient(provider.conn)
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	request := &tenant.TenantListRequest{
		Cluster: clusterName,
	}
	l.Infof("request: '%+v'", *request)
	response, err := c.TenantList(ctx, request)
	if err != nil {
		l.Error(err.Error)
		return nil, err
	}
	l.Infof("response: '%+v'", response)

	for _, info := range response.Info {
		tenants = append(tenants, info.Name)
	}
	l.Debugf("tenants: '%+v'", tenants)

	return tenants, err
}

func (provider *EdgeFSProvider) CreateSnapshot(ss IscsiSnapshotId) (SnapshotInfo, error) {
	l := provider.logger.WithField("func", "CreateSnapshot()")
	client := snapshot.NewSnapshotClient(provider.conn)
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	//TODO: Add validation
	request := &snapshot.SnapshotRequest{
		Snapshot: &snapshot.SnapshotEntry{Cluster: ss.Cluster,
			Tenant: ss.Tenant,
			Bucket: ss.Bucket,
			Object: ss.Object,
			Name:   ss.Name},
	}
	l.Infof("request: '%+v'", *request)

	response, err := client.CreateSnapshot(ctx, request)
	if err != nil {
		l.Error(err.Error)
		return SnapshotInfo{}, err
	}
	l.Infof("response: '%+v'", response)
	return SnapshotInfo{SnapshotPath: response.Snapshot.Name, SourceVolume: response.Snapshot.SourceObject, CreationTime: response.Snapshot.CreationTime}, nil
}

func (provider *EdgeFSProvider) DeleteSnapshot(ss IscsiSnapshotId) (err error) {
	l := provider.logger.WithField("func", "DeleteSnapshot()")
	client := snapshot.NewSnapshotClient(provider.conn)
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	//TODO: Add validation
	request := &snapshot.SnapshotRequest{
		Snapshot: &snapshot.SnapshotEntry{Cluster: ss.Cluster,
			Tenant: ss.Tenant,
			Bucket: ss.Bucket,
			Object: ss.Object,
			Name:   ss.Name,
		},
	}
	l.Infof("request: '%+v'", *request)

	response, err := client.DeleteSnapshot(ctx, request)
	if err != nil {
		l.Error(err.Error)
		return err
	}
	l.Infof("response: '%+v'", response)
	return nil
}

func (provider *EdgeFSProvider) IsSnapshotExists(ss IscsiSnapshotId) (bool, error) {
	l := provider.logger.WithField("func", "IsSnapshotExists()")
	client := snapshot.NewSnapshotClient(provider.conn)
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	objectPath := fmt.Sprintf("%s/%s/%s/%s", ss.Cluster, ss.Tenant, ss.Bucket, ss.Object)
	pattern := fmt.Sprintf("%s@%s", objectPath, ss.Name)
	//TODO: Add validation
	request := &snapshot.SnapshotListRequest{
		Object: &snapshot.ObjectEntry{Cluster: ss.Cluster,
			Tenant: ss.Tenant,
			Bucket: ss.Bucket,
			Object: ss.Object},
		Pattern: pattern,
		Count:   1000,
	}

	l.Infof("request: '%+v'", *request)

	response, err := client.ListSnapshots(ctx, request)
	if err != nil {
		l.Error(err.Error)
		return false, err
	}
	l.Infof("response: '%+v'", response)
	if len(response.Info) > 0 {
		return true, nil
	}
	return false, nil
}

func (provider *EdgeFSProvider) ListSnapshots(volume IscsiVolumeId, pattern string) ([]SnapshotInfo, error) {
	l := provider.logger.WithField("func", "ListSnapshots()")
	client := snapshot.NewSnapshotClient(provider.conn)
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	//TODO: Add validation
	request := &snapshot.SnapshotListRequest{
		Object: &snapshot.ObjectEntry{Cluster: volume.Cluster,
			Tenant: volume.Tenant,
			Bucket: volume.Bucket,
			Object: volume.Object},
		Pattern: pattern,
		Count:   1000,
	}
	l.Infof("request: '%+v'", *request)

	response, err := client.ListSnapshots(ctx, request)
	if err != nil {
		l.Error(err.Error)
		return nil, err
	}
	l.Infof("response: '%+v'", response)

	snapshots := make([]SnapshotInfo, 0)
	for _, info := range response.Info {
		snapshots = append(snapshots, SnapshotInfo{SnapshotPath: info.Name, SourceVolume: info.SourceObject, CreationTime: info.CreationTime})
	}
	l.Infof("snapshots: '%+v'", snapshots)
	return snapshots, nil
}

// CloneVolumeFromSnapshot(sourceSnapshot SnapshotID, cloneVolume EdgefsIscsiVolume
func (provider *EdgeFSProvider) CloneVolumeFromSnapshot(cloneVolume IscsiVolumeId, ss IscsiSnapshotId) (CloneInfo, error) {
	l := provider.logger.WithField("func", "CloneVolumeFromSnapshot()")
	client := snapshot.NewSnapshotClient(provider.conn)
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	//TODO: Add validation
	request := &snapshot.SnapshotCloneRequest{
		Snapshot: &snapshot.SnapshotEntry{
			Name:    ss.Name,
			Cluster: ss.Cluster,
			Tenant:  ss.Tenant,
			Bucket:  ss.Bucket,
			Object:  ss.Object},
		Clone: &snapshot.ObjectEntry{
			Cluster: cloneVolume.Cluster,
			Tenant:  cloneVolume.Tenant,
			Bucket:  cloneVolume.Bucket,
			Object:  cloneVolume.Object},
	}
	l.Infof("request: '%+v'", *request)
	response, err := client.CloneSnapshot(ctx, request)
	if err != nil {
		l.Error(err.Error)
		return CloneInfo{}, err
	}
	l.Infof("response: '%+v'", response)
	snapID := fmt.Sprintf("%s/%s/%s/%s@%s", ss.Cluster, ss.Tenant, ss.Bucket, ss.Object, ss.Name)
	return CloneInfo{SourceSnapshotPath: snapID,
		SourceObjectPath: ss.GetObjectPath(),
		CloneObjectPath:  response.Clone}, nil
}
