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
package main

import (
	"flag"
	"fmt"
	"os"
	"io/ioutil"

	nested "github.com/antonfisher/nested-logrus-formatter"
	logrus "github.com/sirupsen/logrus"

	"github.com/Nexenta/edgefs-csi/csi"
	"github.com/spf13/cobra"
)

func init() {
	flag.Set("logtostderr", "true")
}

var (
	driverType  string
	role        string
	endpoint    string
	nodeID      string
	verbose	    string
)

func main() {
	flag.CommandLine.Parse([]string{})

	cmd := &cobra.Command{
		Use:   "edgefs-csi",
		Short: "CSI based EdgeFS NFS driver",
		Run: func(cmd *cobra.Command, args []string) {
			handle()
		},
	}

	cmd.Flags().AddGoFlagSet(flag.CommandLine)

	cmd.PersistentFlags().StringVar(&driverType, "driverType", "", "EdgeFS CSI driver type [nfs|iscsi]")
        cmd.MarkPersistentFlagRequired("driverType")

	cmd.PersistentFlags().StringVar(&role, "role", "", "EdgeFS CSI driver server type [controller|node]")
	cmd.MarkPersistentFlagRequired("role")

	cmd.PersistentFlags().StringVar(&nodeID, "nodeid", "", "node id")
	cmd.MarkPersistentFlagRequired("nodeid")

	cmd.PersistentFlags().StringVar(&endpoint, "endpoint", "", "CSI endpoint")
	cmd.MarkPersistentFlagRequired("endpoint")

	cmd.PersistentFlags().StringVar(&verbose, "verbose", "", "CSI driver log level")
        cmd.MarkPersistentFlagRequired("verbose")

	cmd.ParseFlags(os.Args[1:])
	if err := cmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "%s", err.Error())
		os.Exit(1)
	}

	os.Exit(0)
}

func handle() {

        // init logger
        l := logrus.New().WithFields(logrus.Fields{
                "nodeID": nodeID,
                "cmp":    "Main",
        })

        // logger formatter
        l.Logger.SetFormatter(&nested.Formatter{
                HideKeys:    true,
                FieldsOrder: []string{"nodeID", "cmp", "func", "req", "reqID", "job"},
        })

	//setup logger verbose level
	loggerLevel, err := logrus.ParseLevel(verbose)
	if err != nil {
                l.Info("Logger level unknown. Set default to Info. %s", err)
		l.Logger.SetLevel(logrus.InfoLevel)
        } else {
		l.Logger.SetLevel(loggerLevel)
	}

	l.Info("Edgefs CSI driver passed CLI options:")
	l.Infof("- Role:             '%s'", role)
	l.Infof("- DriverType:       '%s'", driverType)
	l.Infof("- Node ID:          '%s'", nodeID)
	l.Infof("- CSI endpoint:     '%s'", endpoint)
	l.Infof("- Log verbose:      '%s'", verbose)

	validatedRole, err := csi.ParseRole(role)
	if err != nil {
		l.Warn(err)
	}

	validatedType, err := csi.ParseDriverType(driverType)
	if err != nil {
		l.Warn(err)
	}

	//new driver parameters
	driverParameters := csi.Args{
		Role:       validatedRole,
		DriverType: validatedType,
		NodeID: nodeID,
		Endpoint: endpoint,
		Logger: l,
	}

	l.Infof("New driver args: %+v", driverParameters)

	driver, err := csi.NewDriver(driverParameters)
	if err != nil {
		writeTerminationMessage(err, l)
		l.Fatal(err)
	}

	err = driver.Run()
	if err != nil {
		writeTerminationMessage(err, l)
		l.Fatal(err)
	}
}

// Kubernetes retrieves termination messages from the termination message file of a Container,
// which as a default value of /dev/termination-log
func writeTerminationMessage(err error, l *logrus.Entry) {
	writeErr := ioutil.WriteFile("/tmp/termination-log", []byte(fmt.Sprintf("\n%s\n", err)), os.ModePerm)
	if writeErr != nil {
		l.Warnf("Failed to write termination message: %s", writeErr)
	}
}

