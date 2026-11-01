package controller

import (
	"context"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	cachev1alpha1 "github.com/clambin/redis-controller/api/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/yaml"
)

var update = flag.Bool("update", false, "update golden files")

func TestReconcileCreatesDeploymentAndService(t *testing.T) {
	ctx := context.Background()
	redis := instance()
	r := newReconciler(t, redis)

	_, err := r.Reconcile(ctx, request())
	require.NoError(t, err)

	statefulSet := appsv1.StatefulSet{Name: redis.Name, Namespace: redis.Namespace}
	require.NoError(t, r.Get(ctx, request().NamespacedName, &statefulSet))
	require.Len(t, statefulSet.Spec.Template.Spec.Containers, 1)
	assert.Equal(t, "redis:8", statefulSet.Spec.Template.Spec.Containers[0].Image)
	require.Len(t, statefulSet.Spec.Template.Spec.Containers[0].Ports, 1)
	assert.Equal(t, int32(redisPort), statefulSet.Spec.Template.Spec.Containers[0].Ports[0].ContainerPort)
	require.Len(t, statefulSet.OwnerReferences, 1)
	assert.Equal(t, redis.Name, statefulSet.OwnerReferences[0].Name)

	var svc corev1.Service
	require.NoError(t, r.Get(ctx, request().NamespacedName, &svc))
	assert.Equal(t, corev1.ServiceTypeClusterIP, svc.Spec.Type)
	require.Len(t, svc.Spec.Ports, 1)
	assert.Equal(t, int32(redisPort), svc.Spec.Ports[0].Port)
	assert.Equal(t, redis.Name, svc.Spec.Selector[appLabel])
}

func TestReconcileCreatesDeploymentAndServiceAndExporter(t *testing.T) {
	ctx := context.Background()
	redis := instance()
	redis.Spec.Exporter = new("bitnami/redis-exporter:latest")
	r := newReconciler(t, redis)

	_, err := r.Reconcile(ctx, request())
	require.NoError(t, err)

	var statefulSet appsv1.StatefulSet
	require.NoError(t, r.Get(ctx, request().NamespacedName, &statefulSet))
	require.Len(t, statefulSet.Spec.Template.Spec.Containers, 1)
	assert.Equal(t, "redis:8", statefulSet.Spec.Template.Spec.Containers[0].Image)
	require.Len(t, statefulSet.Spec.Template.Spec.Containers[0].Ports, 1)
	assert.Equal(t, int32(redisPort), statefulSet.Spec.Template.Spec.Containers[0].Ports[0].ContainerPort)
	require.Len(t, statefulSet.OwnerReferences, 1)
	assert.Equal(t, redis.Name, statefulSet.OwnerReferences[0].Name)

	var svc corev1.Service
	require.NoError(t, r.Get(ctx, request().NamespacedName, &svc))
	assert.Equal(t, corev1.ServiceTypeClusterIP, svc.Spec.Type)
	require.Len(t, svc.Spec.Ports, 1)
	assert.Equal(t, int32(redisPort), svc.Spec.Ports[0].Port)
	assert.Equal(t, redis.Name, svc.Spec.Selector[appLabel])

	var deployment appsv1.Deployment
	require.NoError(t, r.Get(ctx, types.NamespacedName{Namespace: redis.Namespace, Name: redis.Name + "-exporter"}, &deployment))
	require.Len(t, deployment.Spec.Template.Spec.Containers, 1)
	assert.Equal(t, "bitnami/redis-exporter:latest", deployment.Spec.Template.Spec.Containers[0].Image)
	require.Len(t, deployment.Spec.Template.Spec.Containers[0].Ports, 1)
	assert.Equal(t, int32(9121), deployment.Spec.Template.Spec.Containers[0].Ports[0].ContainerPort)
	require.Len(t, deployment.OwnerReferences, 1)
	assert.Equal(t, redis.Name, deployment.OwnerReferences[0].Name)
}

func TestReconcileUpdatesImage(t *testing.T) {
	ctx := context.Background()
	redis := instance()
	r := newReconciler(t, redis)

	// sync on default image
	_, err := r.Reconcile(ctx, request())
	require.NoError(t, err)

	// update image and reconcile
	var current cachev1alpha1.Instance
	require.NoError(t, r.Get(ctx, request().NamespacedName, &current))
	current.Spec.Image = "redis:8.2"
	require.NoError(t, r.Update(ctx, &current))
	_, err = r.Reconcile(ctx, request())
	require.NoError(t, err)

	// validate update in deployment
	var statefulSet appsv1.StatefulSet
	require.NoError(t, r.Get(ctx, request().NamespacedName, &statefulSet))
	assert.Equal(t, "redis:8.2", statefulSet.Spec.Template.Spec.Containers[0].Image)
}

func TestReconcileRecreatesMissingChild(t *testing.T) {
	ctx := context.Background()
	r := newReconciler(t, instance())

	// sync
	_, err := r.Reconcile(ctx, request())
	require.NoError(t, err)
	var statefulSet appsv1.StatefulSet
	require.NoError(t, r.Get(ctx, request().NamespacedName, &statefulSet))

	// delete the redis server and reconcile
	require.NoError(t, r.Delete(ctx, &statefulSet))
	_, err = r.Reconcile(ctx, request())
	require.NoError(t, err)

	// redis server was recreated
	require.NoError(t, r.Get(ctx, request().NamespacedName, &statefulSet))
}

func TestReconcileMissingParentDoesNothing(t *testing.T) {
	// set up without parent
	ctx := context.Background()
	r := newReconciler(t)
	_, err := r.Reconcile(ctx, request())
	require.NoError(t, err)

	// no deployment should be created
	var statefulSet appsv1.StatefulSet
	require.True(t, k8serrors.IsNotFound(r.Get(ctx, request().NamespacedName, &statefulSet)))
}

/*
	func TestReconcileUpdatesStatus(t *testing.T) {
		type ready struct {
			server   bool
			exporter bool
		}
		tests := []struct {
			name     string
			exporter *string
			ready    ready
			want     cachev1alpha1.InstanceStatus
		}{
			{
				name: "redis not ready",
				want: cachev1alpha1.InstanceStatus{Message: "Waiting for Redis"},
			},
			{
				name:  "redis ready",
				ready: ready{server: true},
				want:  cachev1alpha1.InstanceStatus{Ready: true, Message: "Redis instance is ready"},
			},
			{
				name:     "exporter not ready",
				exporter: new("bitnami/redis-exporter:latest"),
				ready:    ready{server: true},
				want:     cachev1alpha1.InstanceStatus{Message: "Waiting for Redis exporter"},
			},
			{
				name:     "redis and exporter ready",
				exporter: new("bitnami/redis-exporter:latest"),
				ready:    ready{server: true, exporter: true},
				want:     cachev1alpha1.InstanceStatus{Ready: true, Message: "Redis instance is ready"},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				ctx := context.Background()
				redis := instance()
				redis.Spec.Exporter = tt.exporter
				r := newReconciler(t, redis)

				// initial reconcile creates the deployments, which are not ready yet
				_, err := r.Reconcile(ctx, request())
				require.NoError(t, err)
				var current cachev1alpha1.Instance
				require.NoError(t, r.Get(ctx, request().NamespacedName, &current))
				assert.Equal(t, cachev1alpha1.InstanceStatus{Message: "Waiting for Redis"}, current.Status)

				// mark components as ready and reconcile
				if tt.ready.server {
					var statefulSet appsv1.StatefulSet
					require.NoError(t, r.Get(ctx, types.NamespacedName{Namespace: redis.Namespace, Name: redis.Name}, &statefulSet))
					statefulSet.Status = appsv1.StatefulSetStatus{ObservedGeneration: statefulSet.Generation, UpdatedReplicas: 1, AvailableReplicas: 1}
					require.NoError(t, r.Status().Update(ctx, &statefulSet))
				}
				if tt.ready.exporter {
					var deployment appsv1.Deployment
					require.NoError(t, r.Get(ctx, types.NamespacedName{Namespace: redis.Namespace, Name: redis.Name + "-exporter"}, &deployment))
					deployment.Status = appsv1.DeploymentStatus{ObservedGeneration: deployment.Generation, UpdatedReplicas: 1, AvailableReplicas: 1}
					require.NoError(t, r.Status().Update(ctx, &deployment))
				}

				_, err = r.Reconcile(ctx, request())
				require.NoError(t, err)

				require.NoError(t, r.Get(ctx, request().NamespacedName, &current))
				assert.Equal(t, tt.want, current.Status)
			})
		}
	}
*/
func TestArtifacts_InstanceStatus(t *testing.T) {
	sfsReady := &appsv1.StatefulSet{Status: appsv1.StatefulSetStatus{UpdatedReplicas: 1, AvailableReplicas: 1}}
	sfsNotReady := &appsv1.StatefulSet{}
	sfsOutdated := &appsv1.StatefulSet{Status: appsv1.StatefulSetStatus{UpdatedReplicas: 1, AvailableReplicas: 1}}
	sfsOutdated.Generation = 2
	sfsOutdated.Status.ObservedGeneration = 1
	depReady := &appsv1.Deployment{Status: appsv1.DeploymentStatus{UpdatedReplicas: 1, AvailableReplicas: 1}}
	depNotReady := &appsv1.Deployment{}
	depOutdated := &appsv1.Deployment{Status: appsv1.DeploymentStatus{UpdatedReplicas: 1, AvailableReplicas: 1}}
	depOutdated.Generation = 2
	depOutdated.Status.ObservedGeneration = 1
	depUnavailable := &appsv1.Deployment{Status: appsv1.DeploymentStatus{UpdatedReplicas: 1, AvailableReplicas: 1, UnavailableReplicas: 1}}

	type conditions map[string]metav1.ConditionStatus
	tests := []struct {
		name string
		artifacts
		want conditions
	}{
		{"redis not ready", artifacts{redisServer: sfsNotReady}, conditions{"RedisServer": metav1.ConditionFalse, "Ready": metav1.ConditionFalse}},
		{"redis outdated", artifacts{redisServer: sfsOutdated}, conditions{"RedisServer": metav1.ConditionFalse, "Ready": metav1.ConditionFalse}},
		{"redis ready, no exporter", artifacts{redisServer: sfsReady}, conditions{"RedisServer": metav1.ConditionTrue, "Ready": metav1.ConditionTrue}},
		{"redis not ready, exporter ready", artifacts{redisServer: sfsNotReady, exporter: depReady}, conditions{"RedisServer": metav1.ConditionFalse, "Ready": metav1.ConditionFalse}},
		{"redis ready, exporter not ready", artifacts{redisServer: sfsReady, exporter: depNotReady}, conditions{"Exporter": metav1.ConditionFalse, "Ready": metav1.ConditionFalse}},
		{"redis and exporter ready", artifacts{redisServer: sfsReady, exporter: depReady}, conditions{"RedisServer": metav1.ConditionTrue, "Exporter": metav1.ConditionTrue, "Ready": metav1.ConditionTrue}},
		{"redis ready, exporter has unavailable replica", artifacts{redisServer: sfsReady, exporter: depUnavailable}, conditions{"RedisServer": metav1.ConditionTrue, "Exporter": metav1.ConditionFalse, "Ready": metav1.ConditionFalse}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.artifacts.service = &corev1.Service{}
			status := tt.artifacts.instanceStatus()

			got := make(map[string]metav1.ConditionStatus, len(status.Conditions))
			for _, c := range status.Conditions {
				got[c.Type] = c.Status
			}

			for k, v := range tt.want {
				assert.Equal(t, v, got[k], k)
			}
		})
	}
}

func TestRedisStatefulSet(t *testing.T) {
	tests := []struct {
		name     string
		instance *cachev1alpha1.Instance
	}{
		{
			name: "simple",
			instance: &cachev1alpha1.Instance{
				Name:      "test",
				Namespace: "default",
				Spec: cachev1alpha1.InstanceSpec{
					Image: "redis:latest",
				},
			},
		},
		{
			name: "credentials",
			instance: &cachev1alpha1.Instance{
				Name:      "test",
				Namespace: "default",
				Spec: cachev1alpha1.InstanceSpec{
					Image: "redis:latest",
					Credentials: &cachev1alpha1.RedisCredentials{
						SecretName:  "redis-credentials",
						PasswordKey: "redis-password",
					},
				},
			},
		},
		{
			name: "persistence",
			instance: &cachev1alpha1.Instance{
				Name:      "test",
				Namespace: "default",
				Spec: cachev1alpha1.InstanceSpec{
					Image: "redis:latest",
					Persistence: &corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: "redis-data",
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			statefulSet := appsv1.StatefulSet{
				Name:      tt.instance.Name,
				Namespace: tt.instance.Namespace,
			}
			redisStatefulSet(&statefulSet, tt.instance)
			marshalAndCompare(t, &statefulSet)
		})
	}
}

func TestRedisService(t *testing.T) {
	i := &cachev1alpha1.Instance{
		Name:      "test",
		Namespace: "default",
	}
	svc := corev1.Service{
		Name:      "test",
		Namespace: "default",
	}
	redisService(&svc, i)
	marshalAndCompare(t, &svc)
}

func TestRedisExporterDeployment(t *testing.T) {
	tests := []struct {
		name     string
		instance *cachev1alpha1.Instance
	}{
		{
			name: "no authentication",
			instance: &cachev1alpha1.Instance{
				Name:      "test",
				Namespace: "default",
				Spec: cachev1alpha1.InstanceSpec{
					Exporter: new("foo"),
				},
			},
		},
		{
			name: "authentication",
			instance: &cachev1alpha1.Instance{
				Name:      "test",
				Namespace: "default",
				Spec: cachev1alpha1.InstanceSpec{
					Exporter: new("foo"),
					Credentials: &cachev1alpha1.RedisCredentials{
						SecretName:  "foo",
						PasswordKey: "bar",
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dep := appsv1.Deployment{
				Name:      tt.instance.Name + "-exporter",
				Namespace: tt.instance.Namespace,
			}
			redisExporterDeployment(&dep, tt.instance)
			marshalAndCompare(t, &dep)
		})
	}
}

func marshalAndCompare(tb testing.TB, obj any) {
	tb.Helper()
	body, err := yaml.Marshal(obj)
	require.NoError(tb, err)

	gp := filepath.Join("testdata", strings.ToLower(tb.Name())+".yaml")
	if *update {
		require.NoError(tb, os.MkdirAll(filepath.Dir(gp), 0755))
		require.NoError(tb, os.WriteFile(gp, body, 0644))

	}
	golden, err := os.ReadFile(gp)
	require.NoError(tb, err)
	assert.Equal(tb, string(golden), string(body))
}

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := cachev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return scheme
}

func newReconciler(t *testing.T, objects ...runtime.Object) *RedisInstanceReconciler {
	t.Helper()
	scheme := testScheme(t)
	b := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&cachev1alpha1.Instance{})
	for _, obj := range objects {
		b = b.WithRuntimeObjects(obj)
	}
	return &RedisInstanceReconciler{Client: b.Build(), Scheme: scheme}
}

func request() reconcile.Request {
	return reconcile.Request{Namespace: "default", Name: "cache"}
}

func instance() *cachev1alpha1.Instance {
	return &cachev1alpha1.Instance{
		Name: "cache", Namespace: "default",
		Spec: cachev1alpha1.InstanceSpec{Image: "redis:8"},
	}
}
