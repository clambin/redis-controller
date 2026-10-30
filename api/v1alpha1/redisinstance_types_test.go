package v1alpha1_test

import (
	"testing"

	"github.com/clambin/redis-controller/api/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
)

func TestAddToSchema(t *testing.T) {
	s := runtime.NewScheme()
	require.NoError(t, v1alpha1.AddToScheme(s))
	require.True(t, s.IsVersionRegistered(v1alpha1.GroupVersion))
	knownTypes := s.KnownTypes(v1alpha1.GroupVersion)
	assert.Equal(t, "v1alpha1.Instance", knownTypes["Instance"].String())
	assert.Equal(t, "v1alpha1.InstanceList", knownTypes["InstanceList"].String())
}
