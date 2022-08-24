package controllers

import (
	"encoding/json"
	"fmt"
	"io"
	"io/fs"

	"golang.org/x/sync/errgroup"
	"helm.sh/helm/v3/pkg/chart"
	"helm.sh/helm/v3/pkg/chart/loader"
	"helm.sh/helm/v3/pkg/chartutil"
	"sigs.k8s.io/controller-runtime/pkg/client"

	rukpakv1alpha1 "github.com/operator-framework/rukpak/api/v1alpha1"
	"github.com/operator-framework/rukpak/internal/util"
)

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

func MapToDeployNamespace(obj client.Object) (string, error) {
	bd := obj.(*rukpakv1alpha1.BundleDeployment)
	cfg, err := parseConfig(bd)
	if err != nil {
		return "", err
	}
	return cfg.Namespace, nil
}

type config struct {
	Namespace string           `json:"namespace"`
	Values    chartutil.Values `json:"values"`
}

func parseConfig(bd *rukpakv1alpha1.BundleDeployment) (*config, error) {
	type parseConfig struct {
		Namespace string `json:"namespace"`
		Values    string `json:"values"`
	}
	data, err := bd.Spec.Config.MarshalJSON()
	if err != nil {
		return nil, err
	}
	var pcfg parseConfig
	err = json.Unmarshal(data, &pcfg)
	if err != nil {
		return nil, err
	}

	if pcfg.Namespace == "" {
		return nil, fmt.Errorf("install namespace not defined: set .spec.config.namespace")
	}

	var values chartutil.Values
	if pcfg.Values != "" {
		values, err = chartutil.ReadValues([]byte(pcfg.Values))
		if err != nil {
			return nil, err
		}
	}
	return &config{
		Namespace: pcfg.Namespace,
		Values:    values,
	}, nil
}
