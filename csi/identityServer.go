package csi

import (
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

// IdentityServer - k8s csi driver identity server
type IdentityServer struct {
	driverName    string
	driverVersion string
	log           *logrus.Entry
}

// GetPluginInfo - return plugin info
func (ids *IdentityServer) GetPluginInfo(ctx context.Context, req *csi.GetPluginInfoRequest) (
	*csi.GetPluginInfoResponse,
	error,
) {
	l := ids.log.WithField("func", "GetPluginInfo()")
	l.Infof("request: '%+v'", req)

	res := csi.GetPluginInfoResponse{
		Name:          ids.driverName,
		VendorVersion: ids.driverVersion,
	}

	l.Debugf("response: '%+v'", res)

	return &res, nil
}

// Probe - return driver status (ready or not)
func (ids *IdentityServer) Probe(ctx context.Context, req *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	ids.log.WithField("func", "Probe()").Infof("request: '%+v'", req)

	return &csi.ProbeResponse{}, nil
}

// GetPluginCapabilities - get plugin capabilities
func (ids *IdentityServer) GetPluginCapabilities(ctx context.Context, req *csi.GetPluginCapabilitiesRequest) (
	*csi.GetPluginCapabilitiesResponse,
	error,
) {
	ids.log.WithField("func", "GetPluginCapabilities()").Infof("request: '%+v'", req)

	return &csi.GetPluginCapabilitiesResponse{
		Capabilities: []*csi.PluginCapability{
			{
				Type: &csi.PluginCapability_Service_{
					Service: &csi.PluginCapability_Service{
						Type: csi.PluginCapability_Service_CONTROLLER_SERVICE,
					},
				},
			},
		},
	}, nil
}

// NewIdentityServer - create an instance of identity server
func NewIdentityServer(driver *Driver) *IdentityServer {
	l := driver.logger.WithField("cmp", "IdentityServer")
	l.Info("create new IdentityServer...")

	return &IdentityServer{
		driverName:    driver.name,
		driverVersion: Version,
		log:           l,
	}
}
