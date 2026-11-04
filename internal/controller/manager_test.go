package controller

import (
	"testing"

	"github.com/clambin/redis-controller/api/v1alpha1"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
)

func TestNewManager(t *testing.T) {
	mgr, err := NewManager(&rest.Config{})
	require.NoError(t, err)

	scheme := mgr.GetScheme()
	require.True(t, scheme.IsVersionRegistered(v1alpha1.GroupVersion))
	for _, gvk := range []schema.GroupVersionKind{
		v1alpha1.GroupVersion.WithKind("RedisInstance"),
		// these don't really do what you'd think they do
		appsv1.SchemeGroupVersion.WithKind("Deployment"),
		appsv1.SchemeGroupVersion.WithKind("StatefulSet"),
		corev1.SchemeGroupVersion.WithKind("Service"),
	} {
		require.True(t, scheme.Recognizes(gvk))
	}
}
