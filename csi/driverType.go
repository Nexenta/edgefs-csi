package csi

import (
	"fmt"
)

// DriverType - type of edgefs driver to create [nfs|iscsi]
type DriverType string

const (
	DriverTypeNFS   DriverType = "nfs"
	DriverTypeISCSI DriverType = "iscsi"
)

func (r DriverType) String() string {
	return string(r)
}

// DriverTypes - list of the available driver types
var DriverTypes = []DriverType{DriverTypeNFS, DriverTypeISCSI}

// ParseDriverType - create DriverType from user input
func ParseDriverType(from string) (DriverType, error) {
	for _, driverType := range DriverTypes {
		if string(driverType) == from {
			return driverType, nil
		}
	}
	return DriverTypeNFS, fmt.Errorf(
		"DriverType '%s' is not one of supported types: %v, default '%s' will be used",
		from,
		DriverTypes,
		DriverTypeNFS,
	)
}

