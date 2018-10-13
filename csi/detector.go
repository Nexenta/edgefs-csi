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
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	log "github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	edgefsConfigFile    = "/config/cluster-config.json"
	K8sEdgefsNamespace  = "edgefs"
	K8sEdgefsMgmtPrefix = "edgefs-mgmt"
	K8sEdgefsNfsPrefix  = "edgefs-svc-nfs-"
	k8sClientInCluster = true
)

func IsConfigFileExists() bool {
	if _, err := os.Stat(edgefsConfigFile); os.IsNotExist(err) {
		return false
	}
	return true
}

func ReadParseConfig() (config EdgefsClusterConfig, err error) {

	if !IsConfigFileExists() {
		log.Infof("Config file %s not found", edgefsConfigFile)
		return config, fmt.Errorf("Config file %s not found", edgefsConfigFile)
	}

	content, err := ioutil.ReadFile(edgefsConfigFile)
	if err != nil {
		err = fmt.Errorf("error reading config file: %s error: %s", edgefsConfigFile, err)
		log.Error(err.Error)
		return config, err
	}

	err = json.Unmarshal(content, &config)
	if err != nil {
		err = fmt.Errorf("error parsing config file: %s error: %s", edgefsConfigFile, err)
		log.Error(err.Error)
		return config, err
	}

	return config, nil
}

// Should be deleted in "In-Cluster" build
func homeDir() string {
	if h := os.Getenv("HOME"); h != "" {
		return h
	}
	return os.Getenv("USERPROFILE") // windows
}

/* TODO should be expanded to multiple clusters */
/*
func GetEdgefsCluster() (cluster ClusterData, err error) {

	//check config file exists
	if IsConfigFileExists() {
		log.Infof("Config file %s found", edgefsConfigFile)
		config, err := ReadParseConfig()
		if err != nil {
			err = fmt.Errorf("Error reading config file %s error: %s\n", edgefsConfigFile, err)
			return cluster, err
		}

		log.Infof("StandAloneClusterConfig: %+v ", config)
		cluster = ClusterData{isStandAloneCluster: true, clusterConfig: config, nfsServicesData: make([]NfsServiceData, 0)}
	} else {
		isClusterExists, err := DetectEdgefsK8sCluster()
		if isClusterExists {
			cluster.isStandAloneCluster = false
		}
	}

	return cluster, err
}
*/

/* Will check k8s edgefs cluster existance and will update EdgefsClusterConfig information*/
func DetectEdgefsK8sCluster(config *EdgefsClusterConfig) (clusterExists bool, err error) {
	var kubeconfig string
	var restConfig *rest.Config
	if k8sClientInCluster == true {
		restConfig, err = rest.InClusterConfig()
		if err != nil {
			panic(err.Error())
		}
	} else {
		if home := homeDir(); home != "" {
			kubeconfig = filepath.Join(home, ".kube", "config")
		}
		// use the current context in kubeconfig
		restConfig, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			return false, err
		}
	}

	// create the clientset
	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return false, err
	}

	svcs, err := clientset.CoreV1().Services(K8sEdgefsNamespace).List(metav1.ListOptions{})
	//log.Infof("SVCS: %+v\n", svcs)
	if err != nil {
		log.Errorf("Error: %v\n", err)
		return false, err
	}

	for _, svc := range svcs.Items {
		//log.Infof("Item: %+v\n", svc)

		serviceName := svc.GetName()
		serviceClusterIP := svc.Spec.ClusterIP

		if strings.HasPrefix(serviceName, K8sEdgefsMgmtPrefix) {
			config.EdgefsProxyAddr = serviceClusterIP
			return true, err
		}
	}
	return false, err
}

func GetEdgefsK8sClusterServices() (services []EdgefsService, err error) {
	var kubeconfig string
	var restConfig *rest.Config
	if k8sClientInCluster == true {
		restConfig, err = rest.InClusterConfig()
		if err != nil {
			panic(err.Error())
		}
	} else {
		if home := homeDir(); home != "" {
			kubeconfig = filepath.Join(home, ".kube", "config")
		}
		// use the current context in kubeconfig
		restConfig, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			return services, err
		}
	}

	// create the clientset
	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return services, err
	}

	svcs, err := clientset.CoreV1().Services(K8sEdgefsNamespace).List(metav1.ListOptions{})
	//log.Infof("SVCS: %+v\n", svcs)
	if err != nil {
		log.Errorf("Error: %v\n", err)
		return services, err
	}

	for _, svc := range svcs.Items {
		//log.Infof("Item: %+v\n", svc)

		serviceName := svc.GetName()
		serviceClusterIP := svc.Spec.ClusterIP

		if strings.HasPrefix(serviceName, K8sEdgefsNfsPrefix) {
			//log.Infof("Adding service %s\n", serviceName)
			nfsSvcName := strings.TrimPrefix(serviceName, K8sEdgefsNfsPrefix)
			serviceNetwork := []string{serviceClusterIP}

			newService := EdgefsService{Name: nfsSvcName, ServiceType: "nfs", Status: "enabled", Network: serviceNetwork}
			services = append(services, newService)
		}
	}
	//log.Infof("K8S Edgefs services : %+v\n", services)
	return services, err
}
