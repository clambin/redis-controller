package controller

import (
	"fmt"

	cachev1alpha1 "github.com/clambin/redis-controller/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

func NewManager(config *rest.Config) (manager.Manager, error) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		return nil, fmt.Errorf("corev1.AddToScheme: %w", err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
		return nil, fmt.Errorf("appsv1.AddToScheme: %w", err)
	}
	if err := cachev1alpha1.AddToScheme(scheme); err != nil {
		return nil, fmt.Errorf("cachev1alpha1.AddToScheme: %w", err)
	}

	mgr, err := ctrl.NewManager(config, ctrl.Options{Scheme: scheme})
	if err != nil {
		return nil, fmt.Errorf("ctrl.NewManager: %w", err)
	}

	reconciler := &RedisInstanceReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme()}
	if err = reconciler.SetupWithManager(mgr); err != nil {
		return nil, fmt.Errorf("reconciler.SetupWithManager: %w", err)
	}
	return mgr, nil
}
