package main

import "github.com/opensourceways/sync-repo-file/server"

type configuration struct {
	Clients   []clientConfig          `json:"clients,omitempty"`
	SyncFiles []server.SyncFileConfig `json:"sync_files,omitempty"`
}

func (c *configuration) Validate() error {
	for _, item := range c.SyncFiles {
		if err := item.Validate(); err != nil {
			return nil
		}
	}
	return nil
}

func (c *configuration) SetDefault() {}

type clientConfig struct {
	// Platform is the code platform.
	Platform string `json:"platform" required:"true"`

	// Endpoint is the one of synchronizing file server which is a grpc server.
	Endpoint string `json:"grpc_endpoint"  required:"true"`
}
