package iscsi

type VolumeAccessInfo struct {
	IscsiAccessInfo
	MountOptions string `json:"mountOptions,omitempty"`
}

type IscsiAccessInfo struct {
	IscsiTargetPortal    string   `json:"iscsiTargetPortal,omitempty"`
	IscsiPortals         []string `json:"iscsiPortals,omitempty"`
	IscsiTargetIQN       string   `json:"iscsiTargetIqn,omitempty"`
	IscsiLunNumber       int32    `json:"iscsiLunNumber,omitempty"`
	IscsiInterface       string   `json:"iscsiInterface,omitempty"`
	IscsiIgroup          string   `json:"iscsiIgroup,omitempty"`
	IscsiVAGs            []int64  `json:"iscsiVags,omitempty"`
	IscsiUsername        string   `json:"iscsiUsername,omitempty"`
	IscsiInitiatorSecret string   `json:"iscsiInitiatorSecret,omitempty"`
	IscsiTargetSecret    string   `json:"iscsiTargetSecret,omitempty"`
}

type VolumePublishInfo struct {
	Localhost      bool     `json:"localhost,omitempty"`
	HostIQN        []string `json:"hostIQN,omitempty"`
	HostIP         []string `json:"hostIP,omitempty"`
	HostName       string   `json:"hostName,omitempty"`
	FilesystemType string   `json:"fstype,omitempty"`
	UseCHAP        bool     `json:"useCHAP,omitempty"`
	SharedTarget   bool     `json:"sharedTarget,omitempty"`
	DevicePath     string   `json:"devicePath,omitempty"`
	VolumeAccessInfo
}
