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
	"path/filepath"
        "os"
	"strings"
	"gopkg.in/yaml.v2"
	log "github.com/sirupsen/logrus"
)

const (
        edgefsConfigFolder = "/config"
)

type EdgefsClusterConfig struct {
	Name                  string
	K8sClientInCluster    bool
	EdgefsProxyAddr       string
	EdgefsProxyPort       string
	K8sEdgefsNamespace    string `yaml:"k8sEdgefsNamespace"`
	K8sEdgefsMgmtPrefix   string `yaml:"k8sEdgefsMgmtPrefix"`
	Username              string `yaml:"username"`
	Password              string `yaml:"password"`
	Cluster               string `yaml:"cluster"`
	Tenant                string `yaml:"tenant"`
	Bucket		      string `yaml:"bucket"`// optional bucket name for ISCSI volume creation
	ForceVolumeDeletion   bool   `yaml:"forceVolumeDeletion"`
	ServiceFilter         string `yaml:"serviceFilter"`
	ServiceBalancerPolicy string `yaml:"serviceBalancerPolicy"`
	MountOptions          string `yaml:"mountOptions"`

	//ISCSI specific options
	FsType		      string `yaml:"fsType"`
        ChunkSize	      int32  `yaml:"chunksize"`
        BlockSize             int32  `yaml:"blocksize"`
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


func ReadParseConfig() (config EdgefsClusterConfig, err error) {

	configFilePath, err := findConfigFile(edgefsConfigFolder)

        if err != nil{
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

        return config, nil
}

