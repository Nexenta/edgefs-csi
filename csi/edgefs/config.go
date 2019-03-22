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
 * to yu under the Apache License, Version 2.0 (the
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
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

const (
	edgefsConfigFolder                  = "/config"
	defaultUsername              string = "admin"
	defaultPassword              string = "admin"
	defaultNFSMountOptions       string = "vers=3,tcp"
	defaultK8sEdgefsNamespace    string = "rook-edgefs"
	defaultK8sEdgefsMgmtPrefix   string = "edgefs-mgmt"
	defaultK8sClientInCluster    bool   = true
	defaultFsType                string = "ext4"
	defaultServiceBalancerPolicy string = "minexportspolicy"
	defaultChunkSize             int32  = 16384
	defaultBlockSize             int32  = 4096
)

type EdgefsClusterConfig struct {
	Name                  string
	K8sClientInCluster    bool
	EdgefsProxyAddr       string
	EdgefsProxyPort       string
	K8sEdgefsNamespaces   []string `yaml:"k8sEdgefsNamespaces"`
	K8sEdgefsMgmtPrefix   string   `yaml:"k8sEdgefsMgmtPrefix"`
	Username              string   `yaml:"username"`
	Password              string   `yaml:"password"`
	Service               string   `yaml:"service"`
	Cluster               string   `yaml:"cluster"`
	Tenant                string   `yaml:"tenant"`
	Bucket                string   `yaml:"bucket"` // optional bucket name for ISCSI volume creation
	ForceVolumeDeletion   bool     `yaml:"forceVolumeDeletion"`
	ServiceFilter         string   `yaml:"serviceFilter"`
	ServiceBalancerPolicy string   `yaml:"serviceBalancerPolicy"`
	MountOptions          string   `yaml:"mountOptions"`

	//ISCSI specific options
	FsType    string `yaml:"fsType"`
	ChunkSize int32  `yaml:"chunksize"`
	BlockSize int32  `yaml:"blocksize"`
}

/* GetMountOptions */
func (config *EdgefsClusterConfig) GetMountOptions() (options []string) {

	mountOptionsParts := strings.Split(config.MountOptions, ",")
	for _, option := range mountOptionsParts {
		options = append(options, strings.TrimSpace(option))
	}
	return options
}

func (config *EdgefsClusterConfig) GetServiceFilterMap() (filterMap map[string]bool) {

	if config.ServiceFilter != "" {
		filterMap = make(map[string]bool)
		services := strings.Split(config.ServiceFilter, ",")
		for _, srvName := range services {
			filterMap[strings.TrimSpace(srvName)] = true
		}
	}

	return filterMap
}

func (config *EdgefsClusterConfig) GetSegment(segment string) (string, error) {

	if len(config.K8sEdgefsNamespaces) > 0 {
		if len(strings.TrimSpace(segment)) == 0 {
			return config.K8sEdgefsNamespaces[0], fmt.Errorf("No segment passed. Segment %s should be used", defaultK8sEdgefsNamespace)
		}

		for _, sg := range config.K8sEdgefsNamespaces {
			if strings.TrimSpace(sg) == strings.TrimSpace(segment) {
				return sg, nil
			}
		}
		// returns first namespaces element
		return config.K8sEdgefsNamespaces[0], fmt.Errorf("Segment %s not found in available K8sEdgefsNamespaces list %+v. Segment %s should be used ", segment, config.K8sEdgefsNamespaces, defaultK8sEdgefsNamespace)
	}

	return defaultK8sEdgefsNamespace, fmt.Errorf("No K8sEdgefsNamespaces available in CSI configuration file. Segment %s should be used instead of %s", defaultK8sEdgefsNamespace, segment)
}

func findConfigFile(lookUpDir string) (configFilePath string, err error) {
	err = filepath.Walk(lookUpDir, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}
		ext := filepath.Ext(path)
		if ext == ".yaml" || ext == ".yml" {
			configFilePath = path
			return filepath.SkipDir
		}
		return nil
	})
	return configFilePath, err
}

func LoadEdgefsClusterConfig(backendType string) (config EdgefsClusterConfig, err error) {

	configFilePath, err := findConfigFile(edgefsConfigFolder)

	if err != nil {
		err = fmt.Errorf("Configiration file not found in folder %s", edgefsConfigFolder)
		return config, err
	}

	content, err := ioutil.ReadFile(configFilePath)
	if err != nil {
		err = fmt.Errorf("error reading config file: %s error: %s", configFilePath, err)
		log.Error(err.Error)
		return config, err
	}

	err = yaml.Unmarshal(content, &config)
	if err != nil {
		err = fmt.Errorf("error parsing config file: %s error: %s", configFilePath, err)
		log.Error(err.Error)
		return config, err
	}

	err = PrepareClusterConfigDefaultValues(&config, backendType)
	if err != nil {
		log.WithField("func", "InitEdgeFS()").Errorf("%+v", err)
		return config, err
	}

	return config, nil
}

func PrepareClusterConfigDefaultValues(config *EdgefsClusterConfig, backendType string) error {
	if config == nil {
		return fmt.Errorf("Pointer to ClusterConfig is null")
	}
	config.K8sClientInCluster = defaultK8sClientInCluster

	/* Apply default values here */
	if len(config.Username) == 0 {
		config.Username = defaultUsername
	}

	if len(config.Password) == 0 {
		config.Password = defaultPassword
	}

	if len(config.K8sEdgefsNamespaces) == 0 {
		config.K8sEdgefsNamespaces = append(config.K8sEdgefsNamespaces, defaultK8sEdgefsNamespace)
	}

	if config.K8sEdgefsMgmtPrefix == "" {
		config.K8sEdgefsMgmtPrefix = defaultK8sEdgefsMgmtPrefix
	}

	if backendType == EdgefsServiceType_NFS {
		if len(config.MountOptions) == 0 {
			config.MountOptions = defaultNFSMountOptions
		}
	}

	if config.ChunkSize == 0 {
		config.ChunkSize = defaultChunkSize
	}

	if config.BlockSize == 0 {
		config.BlockSize = defaultBlockSize
	}

	if len(config.FsType) == 0 {
		config.FsType = defaultFsType
	}

	if len(config.ServiceBalancerPolicy) == 0 {
		config.ServiceBalancerPolicy = defaultServiceBalancerPolicy
	}

	return nil
}
