package source

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"github.com/nlepage/go-tarfs"
	"io"
	"io/fs"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	rukpakv1alpha1 "github.com/operator-framework/rukpak/api/v1alpha1"
	"github.com/operator-framework/rukpak/internal/util"
)

type PersistentVolumeClaim struct {
	Client          client.Client
	KubeClient      kubernetes.Interface
	ProvisionerName string
	PodNamespace    string
	UnpackImage     string
}

const pvcBundleUnpackContainerName = "bundle"

func (i *PersistentVolumeClaim) Unpack(ctx context.Context, bundle *rukpakv1alpha1.Bundle) (*Result, error) {
	if bundle.Spec.Source.Type != rukpakv1alpha1.SourceTypePersistentVolumeClaim {
		return nil, fmt.Errorf("bundle source type %q not supported", bundle.Spec.Source.Type)
	}
	if bundle.Spec.Source.PersistentVolumeClaim == nil {
		return nil, fmt.Errorf("bundle source persistentVolumeClaim configuration is unset")
	}

	pod := &corev1.Pod{}
	op, err := i.ensureUnpackPod(ctx, bundle, pod)
	if err != nil {
		return nil, err
	} else if op == controllerutil.OperationResultCreated || op == controllerutil.OperationResultUpdated || pod.DeletionTimestamp != nil {
		return &Result{State: StatePending}, nil
	}

	switch phase := pod.Status.Phase; phase {
	case corev1.PodPending:
		return pendingImagePodResult(pod), nil
	case corev1.PodRunning:
		return &Result{State: StateUnpacking}, nil
	case corev1.PodFailed:
		return nil, i.failedPodResult(ctx, pod)
	case corev1.PodSucceeded:
		return i.succeededPodResult(ctx, bundle, pod)
	default:
		return nil, i.handleUnexpectedPod(ctx, pod)
	}
}

func (i *PersistentVolumeClaim) ensureUnpackPod(ctx context.Context, bundle *rukpakv1alpha1.Bundle, pod *corev1.Pod) (controllerutil.OperationResult, error) {
	controllerRef := metav1.NewControllerRef(bundle, bundle.GroupVersionKind())
	automountServiceAccountToken := false
	pod.SetName(util.PodName(i.ProvisionerName, bundle.Name))
	pod.SetNamespace(i.PodNamespace)

	return util.CreateOrRecreate(ctx, i.Client, pod, func() error {
		pod.SetLabels(map[string]string{
			util.CoreOwnerKindKey: bundle.Kind,
			util.CoreOwnerNameKey: bundle.Name,
		})
		pod.SetOwnerReferences([]metav1.OwnerReference{*controllerRef})
		pod.Spec.AutomountServiceAccountToken = &automountServiceAccountToken
		pod.Spec.RestartPolicy = corev1.RestartPolicyNever

		if len(pod.Spec.Containers) != 1 {
			pod.Spec.Containers = make([]corev1.Container, 1)
		}

		pod.Spec.Containers[0].Name = imageBundleUnpackContainerName
		pod.Spec.Containers[0].Image = i.UnpackImage
		pod.Spec.Containers[0].ImagePullPolicy = corev1.PullIfNotPresent
		pod.Spec.Containers[0].Command = []string{"/unpack", "--bundle-dir", "/bundle"}
		pod.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{{Name: "bundle", MountPath: "/bundle"}}

		pod.Spec.Volumes = []corev1.Volume{
			{Name: "bundle", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
				ClaimName: bundle.Spec.Source.PersistentVolumeClaim.Name,
				ReadOnly:  true,
			}}},
		}
		return nil
	})
}

func (i *PersistentVolumeClaim) failedPodResult(ctx context.Context, pod *corev1.Pod) error {
	logs, err := i.getPodLogs(ctx, pod)
	if err != nil {
		return fmt.Errorf("unpack failed: failed to retrieve failed pod logs: %w", err)
	}
	_ = i.Client.Delete(ctx, pod)
	return fmt.Errorf("unpack failed: %v", string(logs))
}

func (i *PersistentVolumeClaim) succeededPodResult(ctx context.Context, bundle *rukpakv1alpha1.Bundle, pod *corev1.Pod) (*Result, error) {
	bundleFS, err := i.getBundleContents(ctx, pod)
	if err != nil {
		return nil, fmt.Errorf("get bundle contents: %w", err)
	}
	resolvedSource := bundle.Spec.Source.DeepCopy()
	return &Result{Bundle: bundleFS, ResolvedSource: resolvedSource, State: StateUnpacked}, nil
}

func (i *PersistentVolumeClaim) getBundleContents(ctx context.Context, pod *corev1.Pod) (fs.FS, error) {
	bundleData, err := i.getPodLogs(ctx, pod)
	if err != nil {
		return nil, fmt.Errorf("get bundle contents: %w", err)
	}
	bd := struct {
		Content []byte `json:"content"`
	}{}

	if err := json.Unmarshal(bundleData, &bd); err != nil {
		return nil, fmt.Errorf("parse bundle data: %w", err)
	}

	gzr, err := gzip.NewReader(bytes.NewReader(bd.Content))
	if err != nil {
		return nil, fmt.Errorf("read bundle content gzip: %w", err)
	}
	return tarfs.New(gzr)
}

func (i *PersistentVolumeClaim) getBundleImageDigest(pod *corev1.Pod) (string, error) {
	for _, ps := range pod.Status.ContainerStatuses {
		if ps.Name == imageBundleUnpackContainerName && ps.ImageID != "" {
			return ps.ImageID, nil
		}
	}
	return "", fmt.Errorf("bundle image digest not found")
}

func (i *PersistentVolumeClaim) getPodLogs(ctx context.Context, pod *corev1.Pod) ([]byte, error) {
	logReader, err := i.KubeClient.CoreV1().Pods(pod.Namespace).GetLogs(pod.Name, &corev1.PodLogOptions{}).Stream(ctx)
	if err != nil {
		return nil, fmt.Errorf("get pod logs: %w", err)
	}
	defer logReader.Close()
	buf := &bytes.Buffer{}
	if _, err := io.Copy(buf, logReader); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (i *PersistentVolumeClaim) handleUnexpectedPod(ctx context.Context, pod *corev1.Pod) error {
	_ = i.Client.Delete(ctx, pod)
	return fmt.Errorf("unexpected pod phase: %v", pod.Status.Phase)
}
