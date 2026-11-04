// +kubebuilder:object:generate=true
// +groupName=redis.agrajag.be
// +kubebuilder:rbac:groups=redis.agrajag.be,resources=redisinstances,verbs=get;list;watch
// +kubebuilder:rbac:groups=redis.agrajag.be,resources=redisinstances/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=redis.agrajag.be,resources=redisinstances/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="apps",resources=deployments;statefulsets,verbs=get;list;watch;create;update;patch;delete

// Package v1alpha1 contains the API schema for RedisInstance.
package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

//go:generate go tool controller-gen object paths=.
//go:generate go tool controller-gen crd paths=. output:crd:dir=../../manifests/crd
//go:generate go tool controller-gen rbac:roleName=redis-controller paths=. output:rbac:dir=../../manifests/rbac

var GroupVersion = schema.GroupVersion{Group: "redis.agrajag.be", Version: "v1alpha1"}

func AddToScheme(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(GroupVersion, &RedisInstance{}, &RedisInstanceList{})
	metav1.AddToGroupVersion(scheme, GroupVersion)
	return nil
}

// RedisInstance is the schema for the Redis instances API.
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
type RedisInstance struct {
	Spec              InstanceSpec `json:"spec,omitempty"`
	metav1.TypeMeta   `json:",inline"`
	Status            InstanceStatus `json:"status,omitempty"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
}

// InstanceSpec describes a single Redis server managed by this controller.
type InstanceSpec struct {
	// Server is the Redis server configuration.
	Server ServerSpec `json:"server"`
	// Exporter is the Redis exporter configuration. If not specified, the instances runs without exporter.
	// +optional
	Exporter *ExporterSpec `json:"exporter,omitempty"`
	// Credentials define the Secret containing the credentials.
	// If the Secret is not specified, Redis runs without authentication.
	// +optional
	Credentials *RedisCredentials `json:"credentials,omitempty"`
	// Persistence is the Redis persistence configuration. If specified, Redis runs with persistence
	// (i.e., --appendonly yes --appendfsync everysec). If no persistence is specified, Redis runs without persistence.
	// +optional
	Persistence *corev1.VolumeSource `json:"persistence,omitempty"`
}

type ServerSpec struct {
	// Image is the Redis container image, for example, redis:8.
	// +kubebuilder:validation:MinLength=1
	Image string `json:"image"`
	// Resources defines the resource requirements for the Redis server.
	// +kubebuilder:validation:XValidation:rule="!has(self.claims) || size(self.claims) == 0",message="DRA resource claims are not supported"
	// +optional
	Resources *corev1.ResourceRequirements `json:"resources"`
}

type ExporterSpec struct {
	// Image is the Redis exporter container image, for example, bitnami/redis-exporter:latest
	// +kubebuilder:validation:MinLength=1
	Image string `json:"image"`
	// Resources defines the resource requirements for the Redis exporter.
	// +kubebuilder:validation:XValidation:rule="!has(self.claims) || size(self.claims) == 0",message="DRA resource claims are not supported"
	// +optional
	Resources *corev1.ResourceRequirements `json:"resources"`
}

type RedisCredentials struct {
	// SecretName references the Secret containing the credentials.
	// +kubebuilder:validation:MinLength=1
	SecretName string `json:"secretName"`
	// PasswordKey is the Secret key containing the Redis password.
	// +kubebuilder:validation:MinLength=1
	PasswordKey string `json:"passwordKey"`
}

// InstanceStatus contains the status of a RedisInstance resource.
type InstanceStatus struct {
	// Conditions represent the observed state of the Redis instance.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// RedisInstanceList contains a list of RedisInstance resources.
// +kubebuilder:object:root=true
type RedisInstanceList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []RedisInstance `json:"items"`
}
