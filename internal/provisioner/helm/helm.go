package helm

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"

	"golang.org/x/sync/errgroup"
	"helm.sh/helm/v3/pkg/chart"
	"helm.sh/helm/v3/pkg/chart/loader"
	"helm.sh/helm/v3/pkg/chartutil"

	rukpakv1alpha1 "github.com/operator-framework/rukpak/api/v1alpha1"
	"github.com/operator-framework/rukpak/internal/util"
)

const (
	// ProvisionerID is the unique helm provisioner ID
	ProvisionerID = "core-rukpak-io-helm"
)

func HandleBundle(ctx context.Context, fsys fs.FS, bundle *rukpakv1alpha1.Bundle) (fs.FS, error) {
	// Helm expects an FS whose root contains a single chart directory. Depending on how
	// the bundle is sourced, the FS may or may not contain this single chart directory in
	// its root (e.g. charts uploaded via 'rukpakctl run <bdName> <chartDir>') would not.
	// This FS wrapper adds this base directory unless the FS already has a base directory.
	chartFS, err := util.EnsureBaseDirFS(fsys, "chart")
	if err != nil {
		return nil, err
	}

	if _, err = getChart(chartFS); err != nil {
		return nil, err
	}
	return chartFS, nil
}

func HandleBundleDeployment(ctx context.Context, fsys fs.FS, bd *rukpakv1alpha1.BundleDeployment) (*chart.Chart, chartutil.Values, error) {
	cfg, err := ParseConfig(bd)
	if err != nil {
		return nil, nil, fmt.Errorf("parse bundle deployment config: %v", err)
	}
	chart, err := getChart(fsys)
	if err != nil {
		return nil, nil, err
	}
	return chart, cfg.Values, nil
}

type Config struct {
	Namespace string
	Values    chartutil.Values
}

func ParseConfig(bd *rukpakv1alpha1.BundleDeployment) (*Config, error) {
	type config struct {
		Namespace string `json:"namespace,omitempty"`
		Values    string `json:"values,omitempty"`
	}

	data, err := json.Marshal(bd.Spec.Config)
	if err != nil {
		return nil, fmt.Errorf("marshal bundle deployment config to JSON: %v", err)
	}

	var cfg config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	valuesString := cfg.Values

	var values chartutil.Values
	if cfg.Values != "" {
		var err error
		values, err = chartutil.ReadValues([]byte(valuesString))
		if err != nil {
			return nil, fmt.Errorf("read chart values: %v", err)
		}
	}

	return &Config{
		Namespace: cfg.Namespace,
		Values:    values,
	}, nil
}

func getChart(chartfs fs.FS) (*chart.Chart, error) {
	pr, pw := io.Pipe()
	var eg errgroup.Group
	eg.Go(func() error {
		return pw.CloseWithError(util.FSToTarGZ(pw, chartfs))
	})

	var chrt *chart.Chart
	eg.Go(func() error {
		var err error
		chrt, err = loader.LoadArchive(pr)
		if err != nil {
			return err
		}
		return chrt.Validate()
	})
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	return chrt, nil
}
