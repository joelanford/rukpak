package catalogd

import (
	"context"
	"io/fs"

	"github.com/operator-framework/operator-registry/alpha/declcfg"

	rukpakv1alpha1 "github.com/operator-framework/rukpak/api/v1alpha1"
)

const (
	// FBCProvisionerID is the unique catalogd FBC provisioner ID
	FBCProvisionerID = "catalogd-operatorframework-io-fbc"
)

func HandleBundle(_ context.Context, fsys fs.FS, _ *rukpakv1alpha1.Bundle) (fs.FS, error) {
	fbc, err := declcfg.LoadFS(fsys)
	if err != nil {
		return nil, err
	}
	if _, err := declcfg.ConvertToModel(*fbc); err != nil {
		return nil, err
	}
	return fsys, nil
}
