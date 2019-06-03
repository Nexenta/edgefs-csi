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
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"path"
	"path/filepath"

	iscsi "github.com/Nexenta/edgefs-csi/csi/drivers/iscsi"
	nfs "github.com/Nexenta/edgefs-csi/csi/drivers/nfs"

	"github.com/container-storage-interface/spec/lib/go/csi"
	logrus "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

var (
	version = "1.0.0"
)

const (
	EdgefsNfsDriverName   = "io.edgefs.csi.nfs"
	EdgefsIscsiDriverName = "io.edgefs.csi.iscsi"
)

// Version - driver version, to set version set flags:
// go build -ldflags "-X github.com/Nexenta/edgefs/src/csi/edgefs-csi/csi.Version=0.0.1"
var Version string

// Commit - driver last commit, to set commit set flags:
// go build -ldflags "-X github.com/Nexenta/edgefs/src/csi/edgefs-csi/csi.Commit=..."
var Commit string

// DateTime - driver build datetime, to set commit set flags:
// go build -ldflags "-X github.com/Nexenta/edgefs/src/csi/edgefs-csi/csi.DateTime=..."
var DateTime string

type Driver struct {
	name       string
	role       Role
	driverType DriverType
	endpoint   string
	nodeID     string
	server     *grpc.Server
	logger     *logrus.Entry
}

type Args struct {
	Role       Role
	DriverType DriverType
	NodeID     string
	Endpoint   string
	Logger     *logrus.Entry
}

func NewDriver(args Args) (*Driver, error) {
	l := args.Logger.WithField("cmp", "Driver")

	var driverName string
	switch args.DriverType {
	case DriverTypeNFS:
		driverName = EdgefsNfsDriverName
	case DriverTypeISCSI:
		driverName = EdgefsIscsiDriverName
	default:
		l.Errorf("Unknown driverType: %s", args.DriverType)
		return nil, fmt.Errorf("Unknown driverType: %s", args.DriverType)
	}

	l.Infof("create new %s csi driver: %s@%s-%s (%s)", args.DriverType, driverName, Version, Commit, DateTime)

	d := &Driver{
		name:       driverName,
		role:       args.Role,
		driverType: args.DriverType,
		endpoint:   args.Endpoint,
		nodeID:     args.NodeID,
		logger:     l,
	}

	return d, nil
}

func (d *Driver) Run() error {
	d.logger.Info("run")

	parsedURL, err := url.Parse(d.endpoint)
	if err != nil {
		return fmt.Errorf("Failed to parse endpoint: %s", d.endpoint)
	}

	if parsedURL.Scheme != "unix" {
		return fmt.Errorf("Only unix domain sockets are supported")
	}

	socket := filepath.FromSlash(parsedURL.Path)
	if parsedURL.Host != "" {
		socket = path.Join(parsedURL.Host, socket)
	}

	d.logger.Infof("parsed unix domain socket: %s", socket)

	//remove old socket file if exists
	if err := os.Remove(socket); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("Cannot remove unix domain socket: %s", socket)
	}

	listener, err := net.Listen(parsedURL.Scheme, socket)
	if err != nil {
		return fmt.Errorf("Failed to create socket listener: %s", err)
	}

	d.server = grpc.NewServer(grpc.UnaryInterceptor(d.grpcErrorHandler))

	// IdentityServer - should be running on both controller and node pods
	csi.RegisterIdentityServer(d.server, NewIdentityServer(d))

	if d.role.IsController() {
		controllerServer, err := NewControllerServer(d)
		if err != nil {
			return fmt.Errorf("Failed to create ControllerServer: %s", err)
		}
		csi.RegisterControllerServer(d.server, controllerServer)
	}

	if d.role.IsNode() {
		nodeServer, err := NewNodeServer(d)
		if err != nil {
			return fmt.Errorf("Failed to create NodeServer: %s", err)
		}
		csi.RegisterNodeServer(d.server, nodeServer)
	}

	return d.server.Serve(listener)
}

/*NewControllerServer created commin controller server */
func NewControllerServer(d *Driver) (csi.ControllerServer, error) {
	d.logger.Infof("NewControllerServer DriverType: %s", d.driverType)
	switch d.driverType {
	case DriverTypeISCSI:
		return &iscsi.ControllerServer{
			Logger: d.logger,
		}, nil
	case DriverTypeNFS:
		return &nfs.ControllerServer{
			Logger: d.logger,
		}, nil
	}

	return nil, fmt.Errorf("Unknown driver type %s for Controller server", d.driverType)
}

/*NewNodeServer creates new default Node server */
func NewNodeServer(d *Driver) (csi.NodeServer, error) {
	d.logger.Infof("NewNodeServer DriverType: %s", d.driverType)
	switch d.driverType {
	case DriverTypeISCSI:
		return &iscsi.NodeServer{
			NodeID: d.nodeID,
			Logger: d.logger,
		}, nil
	case DriverTypeNFS:
		return &nfs.NodeServer{
			NodeID: d.nodeID,
			Logger: d.logger,
		}, nil
	}
	return nil, fmt.Errorf("Unknown driver type %s for NodeServer", d.driverType)
}

func (d *Driver) grpcErrorHandler(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	resp, err := handler(ctx, req)
	if err != nil {
		d.logger.WithField("func", "grpc").Error(err)
	}
	return resp, err
}
