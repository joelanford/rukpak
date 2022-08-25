/*
Copyright © 2022 NAME HERE <EMAIL ADDRESS>

*/
package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/manager/signals"

	plain "github.com/operator-framework/rukpak/internal/provisioner/plain/types"
	"github.com/operator-framework/rukpak/internal/rukpakctl"
)

// newRunCmd creates the run command
func newRunCmd() *cobra.Command {
	var (
		systemNamespace                      string
		uploadServiceName                    string
		caSecretName                         string
		bundleDeploymentConfig               string
		bundleDeploymentProvisionerClassName string
		bundleProvisionerClassName           string
	)

	cmd := &cobra.Command{
		Use:   "run <bundleDeploymentName> <bundleDir>",
		Short: "Run a bundle from an upload of a local bundle directory.",
		Long: `Run a bundle from an upload of a local bundle directory.

The run subcommand allows bundle developers to quickly iterate on bundles
they are developing, and to test how their bundle deployment pivots from
one version to the next.
`,
		Example: `
  #
  # Initial creation of memcached-api bundle deployment:
  #
  $ rukpakctl run memcached-api ./memcached-api-v0.1.0/
  bundledeployment.core.rukpak.io "memcached-api" applied
  successfully uploaded bundle content for "memcached-api-5b9bbf8799"

  #
  # Pivot to a new bundle for the existing memcached-api bundle-deployment
  #
  $ rukpakctl run memcached-api ./memcached-api-v0.2.0/
  bundledeployment.core.rukpak.io "memcached-api" applied
  successfully uploaded bundle content for "memcached-api-8578dfddf9"

  #
  # Run the same command again
  #
  $ rukpakctl run memcached-api ./memcached-api-v0.2.0/
  bundledeployment.core.rukpak.io "memcached-api" applied
  bundle "memcached-api-8578dfddf9" is already up-to-date

  #
  # Install a helm chart with a custom config
  #
  $ helm create nginx
  $ kubectl create namespace nginx-system
  $ rukpakctl run nginx ./nginx/                                \
      --bundle-deployment-provisioner-class=core-rukpak-io-helm \
      --bundle-provisioner-class=core-rukpak-io-helm            \
      --config='{"namespace":"nginx-system"}'
  bundledeployment.core.rukpak.io "nginx" applied
  successfully uploaded bundle content for "nginx-f4wfq47tg4"
`,
		Args: cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			bundleDeploymentName, bundleDir := args[0], args[1]
			ctx := signals.SetupSignalHandler()

			cfg := ctrl.GetConfigOrDie()

			r := rukpakctl.Run{
				Config:            cfg,
				SystemNamespace:   systemNamespace,
				UploadServiceName: uploadServiceName,
				CASecretName:      caSecretName,
			}

			bdConfig, err := loadBundleDeploymentConfig(bundleDeploymentConfig)
			if err != nil {
				log.Fatal(err)
			}

			if _, err := r.Run(ctx, bundleDeploymentName, os.DirFS(bundleDir), rukpakctl.RunOptions{
				BundleDeploymentConfig:               bdConfig,
				BundleDeploymentProvisionerClassName: bundleDeploymentProvisionerClassName,
				BundleProvisionerClassName:           bundleProvisionerClassName,
				Log:                                  func(format string, a ...interface{}) { fmt.Printf(format, a...) },
			}); err != nil {
				log.Fatal(err)
			}
		},
	}
	cmd.Flags().StringVar(&systemNamespace, "system-namespace", "rukpak-system", "Namespace in which the rukpak controllers are deployed.")
	cmd.Flags().StringVar(&uploadServiceName, "upload-service-name", "core", "Name of the service of the upload manager.")
	cmd.Flags().StringVar(&caSecretName, "ca-secret-name", "rukpak-ca", "Name of the secret in the system namespace containing the root CAs used to authenticate the upload service.")
	cmd.Flags().StringVar(&bundleDeploymentConfig, "config", "", "JSON data to set on bundle deployment config. '--config=@config.json' loads config from 'config.json'")
	cmd.Flags().StringVar(&bundleDeploymentProvisionerClassName, "bundle-deployment-provisioner-class", plain.ProvisionerID, "Provisioner class name to set on bundle deployment.")
	cmd.Flags().StringVar(&bundleProvisionerClassName, "bundle-provisioner-class", plain.ProvisionerID, "Provisioner class name to set on bundle.")
	return cmd
}

func loadBundleDeploymentConfig(input string) (*runtime.RawExtension, error) {
	if len(input) == 0 {
		return nil, nil
	}
	var (
		reader io.Reader
		err    error
	)
	if strings.HasPrefix(input, "@") {
		reader, err = os.Open(strings.TrimPrefix(input, "@"))
		if err != nil {
			return nil, err
		}
	} else {
		reader = bytes.NewBufferString(input)
	}
	config := &runtime.RawExtension{}
	if err := json.NewDecoder(reader).Decode(config); err != nil {
		return nil, err
	}
	return config, nil
}
