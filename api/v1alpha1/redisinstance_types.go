// +kubebuilder:object:generate=true
// +groupName=redis.agrajag.be
// +kubebuilder:rbac:groups=redis.agrajag.be,resources=instances,verbs=get;list;watch
// +kubebuilder:rbac:groups=redis.agrajag.be,resources=instances/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=redis.agrajag.be,resources=instances/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get
// +kubebuilder:rbac:groups="",resources=deployments;services,verbs=get;list;watch;create;update;patch;delete

// Package v1alpha1 contains the API schema for Instance.
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
	scheme.AddKnownTypes(GroupVersion, &Instance{}, &InstanceList{})
	metav1.AddToGroupVersion(scheme, GroupVersion)
	return nil
}

// Instance is the schema for the Redis instances API.
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
type Instance struct {
	Spec              Spec `json:"spec,omitempty"`
	metav1.TypeMeta   `json:",inline"`
	Status            Status `json:"status,omitempty"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
}

// Spec describes a single Redis server managed by this controller.
type Spec struct {
	// Credentials define the Secret containing the credentials.
	// If the Secret is not specified, Redis runs without authentication.
	// +optional
	Credentials *Credentials `json:"credentials,omitempty"`
	// Exporter is the Redis exporter container image, for example, bitnami/redis-exporter:latest
	// If the Exporter is not specified, the controller will not create a Redis exporter.
	// +kubebuilder:validation:MinLength=1
	// +optional
	Exporter *string `json:"exporter,omitempty"`
	// Persistence is the Redis persistence configuration. If specified, Redis runs with persistence
	// (i.e., --appendonly yes --appendfsync everysec). If no persistence is specified, Redis runs without persistence.
	// +optional
	Persistence *corev1.VolumeSource `json:"persistence,omitempty"`
	// Image is the Redis container image, for example, redis:8.
	// +kubebuilder:validation:MinLength=1
	Image string `json:"image"`
}

type Credentials struct {
	// SecretName references the Secret containing the credentials.
	// +kubebuilder:validation:MinLength=1
	SecretName string `json:"secretName"`
	// PasswordKey is the Secret key containing the Redis password.
	// +kubebuilder:validation:MinLength=1
	PasswordKey string `json:"passwordKey"`
}

// Status contains the status of a RedisInstance resource.
type Status struct {
	// +optional
	Message string `json:"message,omitempty"`
	// +optional
	Ready bool `json:"ready,omitempty"`
}

// InstanceList contains a list of Instance resources.
// +kubebuilder:object:root=true
type InstanceList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Instance `json:"items"`
}
