package controller

import (
	"testing"

	"github.com/clambin/redis-controller/api/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/rest"
)

func TestNewManager(t *testing.T) {
	mgr, err := NewManager(&rest.Config{})
	require.NoError(t, err)
	s := mgr.GetScheme()
	knownTypes := s.KnownTypes(v1alpha1.GroupVersion)
	assert.Equal(t, "v1alpha1.Instance", knownTypes["Instance"].String())
	assert.Equal(t, "v1alpha1.InstanceList", knownTypes["InstanceList"].String())
}
