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
	"os"
	"path/filepath"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

/* represents Kubernetes Edgefs service information [nfs|iscsi]*/
type IK8SEdgefsService interface {
	GetName() string
	GetK8SSvcName() string
	GetK8SNamespace() string
	GetClusterIP() string
	GetPort() string
	GetType() string
}

type K8SEdgefsService struct {
	Name         string
	K8SSvcName   string
	K8SNamespace string
	ServiceType  string
	ClusterIP    string
	Port         string
}

func (svc *K8SEdgefsService) GetName() string {
	return svc.Name
}

func (svc *K8SEdgefsService) GetK8SSvcName() string {
	return svc.K8SSvcName
}

func (svc *K8SEdgefsService) GetK8SNamespace() string {
	return svc.K8SNamespace
}

func (svc *K8SEdgefsService) GetType() string {
	return svc.ServiceType
}

func (svc *K8SEdgefsService) GetClusterIP() string {
	return svc.ClusterIP
}

func (svc *K8SEdgefsService) GetPort() string {
	return svc.Port
}

func (svc *K8SEdgefsService) String() string {
	return fmt.Sprintf("{Name: %s, K8SSvcName: %s, K8SNamespace: %s, ServiceType: %s, ClusterIP: %s, Port: %s}", svc.Name,
		svc.K8SSvcName,
		svc.K8SNamespace,
		svc.ServiceType,
		svc.ClusterIP,
		svc.Port)
}

func NewK8SEdgefsNfsService(name, k8sSvcName, k8sNamespace, clusterIP, port string) IK8SEdgefsService {
	return &K8SEdgefsService{Name: name, K8SSvcName: k8sSvcName, K8SNamespace: k8sNamespace, ServiceType: EdgefsServiceType_NFS, ClusterIP: clusterIP, Port: port}
}

func NewK8SEdgefsIscsiService(name, k8sSvcName, k8sNamespace, clusterIP, port string) IK8SEdgefsService {
	return &K8SEdgefsService{Name: name, K8SSvcName: k8sSvcName, K8SNamespace: k8sNamespace, ServiceType: EdgefsServiceType_ISCSI, ClusterIP: clusterIP, Port: port}
}

// Should be deleted in "In-Cluster" build
func homeDir() string {
	if h := os.Getenv("HOME"); h != "" {
		return h
	}
	return os.Getenv("USERPROFILE") // windows
}

/* Will check k8s edgefs cluster existance and will update EdgefsClusterConfig information*/
func DetectEdgefsK8sCluster(segment string, config *EdgefsClusterConfig) (err error) {
	var kubeconfig string
	var restConfig *rest.Config

	if config.K8sClientInCluster {
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
			return err
		}
	}

	// create the clientset
	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return err
	}

	svcs, err := clientset.CoreV1().Services(segment).List(metav1.ListOptions{})
	if err != nil {
		return err
	}

	for _, svc := range svcs.Items {
		serviceName := svc.GetName()
		serviceClusterIP := svc.Spec.ClusterIP

		if strings.HasPrefix(serviceName, config.K8sEdgefsMgmtPrefix) {
			config.EdgefsProxyAddr = serviceClusterIP
			return nil
		}
	}
	return fmt.Errorf("No Kubernetes' Edgefs service found in '%s' namespace", segment)
}

func GetEdgefsK8sClusterServices(serviceType, k8sEdgefsNamespace string, k8sClientInCluster bool) (services []IK8SEdgefsService, err error) {
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
	svcs, err := clientset.CoreV1().Services(k8sEdgefsNamespace).List(metav1.ListOptions{})
	if err != nil {
		return services, err
	}

	for _, svc := range svcs.Items {

		k8sServiceName := svc.GetName()
		k8sNamespace := svc.GetNamespace()
		serviceClusterIP := svc.Spec.ClusterIP
		if typeValue, ok := svc.Labels["edgefs_svctype"]; ok {
			//only one service type allowed to be in service list
			if typeValue != serviceType {
				continue
			}

			//TODO: parse service ports
			if efsServiceName, ok := svc.Labels["edgefs_svcname"]; ok {
				var newService IK8SEdgefsService
				switch typeValue {
				case EdgefsServiceType_NFS:
					newService = NewK8SEdgefsNfsService(efsServiceName, k8sServiceName, k8sNamespace, serviceClusterIP, "2049")
				case EdgefsServiceType_ISCSI:
					newService = NewK8SEdgefsIscsiService(efsServiceName, k8sServiceName, k8sNamespace, serviceClusterIP, "3260")
				default:
					continue

				}

				services = append(services, newService)
			}
		}
	}
	return services, err
}
