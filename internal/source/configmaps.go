package source

import (
	"context"
	"fmt"
	"path/filepath"
	"testing/fstest"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	rukpakv1alpha1 "github.com/operator-framework/rukpak/api/v1alpha1"
)

type ConfigMaps struct {
	Reader             client.Reader
	ConfigMapNamespace string
}

func (o *ConfigMaps) Unpack(ctx context.Context, bundle *rukpakv1alpha1.Bundle) (*Result, error) {
	if bundle.Spec.Source.Type != rukpakv1alpha1.SourceTypeConfigMaps {
		return nil, fmt.Errorf("bundle source type %q not supported", bundle.Spec.Source.Type)
	}
	if bundle.Spec.Source.ConfigMaps == nil {
		return nil, fmt.Errorf("bundle source configmaps configuration is unset")
	}

	configMapSources := bundle.Spec.Source.ConfigMaps

	bundleFS := fstest.MapFS{}
	for _, cmSource := range configMapSources {
		cmName := cmSource.ConfigMap.Name
		dir := filepath.Clean(cmSource.Path)

		// Check for paths outside the bundle root is handled in the bundle validation webhook
		// if strings.HasPrefix("../", dir) { ... }

		var cm corev1.ConfigMap
		if err := o.Reader.Get(ctx, client.ObjectKey{Name: cmName, Namespace: o.ConfigMapNamespace}, &cm); err != nil {
			return nil, fmt.Errorf("get configmap %s/%s: %v", o.ConfigMapNamespace, cmName, err)
		}

		// TODO: move configmaps immutability check to webhook
		//   This would require the webhook to lookup referenced configmaps.
		if cm.Immutable == nil || *cm.Immutable == false {
			return nil, fmt.Errorf("configmap %s/%s is not immutable: all bundle configmaps must be immutable", o.ConfigMapNamespace, cmName)
		}

		// TODO: we should also forbid deletion of configmaps referenced by bundles
		//   This would require a new validating webhook configuration and deletion handler for configmaps.
		//   Without this check, an immutable configmap could still be deleted and recreated. Whenever the bundle
		//   is reconciled again after that (e.g. due to provisioner restart or a watch on configmaps), the new
		//   configmap content will be unpacked. At that point, if a BundleDeployment references the Bundle either:
		//     - the deployed objects managed by the BD will be out of sync
		//     - the BD will pivot inplace with the existing bundle (which shouldn't happen)

		files := map[string][]byte{}
		for filename, data := range cm.Data {
			files[filename] = []byte(data)
		}
		for filename, data := range cm.BinaryData {
			files[filename] = data
		}

		seenFilepaths := map[string]string{}
		for filename, data := range files {
			filepath := filepath.Join(dir, filename)

			// forbid multiple configmaps in the list from referencing the same destination file.
			if existingCmName, ok := seenFilepaths[filepath]; ok {
				return nil, fmt.Errorf("configmap %s/%s contains path %q which is already referenced by configmap %s/%s",
					o.ConfigMapNamespace, cmName, filepath, o.ConfigMapNamespace, existingCmName)
			}
			seenFilepaths[filepath] = cmName
			bundleFS[filepath] = &fstest.MapFile{
				Data: data,
			}
		}
	}

	resolvedSource := &rukpakv1alpha1.BundleSource{
		Type:       rukpakv1alpha1.SourceTypeConfigMaps,
		ConfigMaps: bundle.Spec.Source.DeepCopy().ConfigMaps,
	}

	return &Result{Bundle: bundleFS, ResolvedSource: resolvedSource, State: StateUnpacked}, nil
}
