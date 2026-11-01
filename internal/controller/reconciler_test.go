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

	dep := appsv1.Deployment{Name: redis.Name, Namespace: redis.Namespace}
	require.NoError(t, r.Get(ctx, request().NamespacedName, &dep))
	require.Len(t, dep.Spec.Template.Spec.Containers, 1)
	assert.Equal(t, "redis:8", dep.Spec.Template.Spec.Containers[0].Image)
	require.Len(t, dep.Spec.Template.Spec.Containers[0].Ports, 1)
	assert.Equal(t, int32(redisPort), dep.Spec.Template.Spec.Containers[0].Ports[0].ContainerPort)
	require.Len(t, dep.OwnerReferences, 1)
	assert.Equal(t, redis.Name, dep.OwnerReferences[0].Name)

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

	var dep appsv1.Deployment
	require.NoError(t, r.Get(ctx, request().NamespacedName, &dep))
	require.Len(t, dep.Spec.Template.Spec.Containers, 1)
	assert.Equal(t, "redis:8", dep.Spec.Template.Spec.Containers[0].Image)
	require.Len(t, dep.Spec.Template.Spec.Containers[0].Ports, 1)
	assert.Equal(t, int32(redisPort), dep.Spec.Template.Spec.Containers[0].Ports[0].ContainerPort)
	require.Len(t, dep.OwnerReferences, 1)
	assert.Equal(t, redis.Name, dep.OwnerReferences[0].Name)

	var svc corev1.Service
	require.NoError(t, r.Get(ctx, request().NamespacedName, &svc))
	assert.Equal(t, corev1.ServiceTypeClusterIP, svc.Spec.Type)
	require.Len(t, svc.Spec.Ports, 1)
	assert.Equal(t, int32(redisPort), svc.Spec.Ports[0].Port)
	assert.Equal(t, redis.Name, svc.Spec.Selector[appLabel])

	require.NoError(t, r.Get(ctx, types.NamespacedName{Namespace: redis.Namespace, Name: redis.Name + "-exporter"}, &dep))
	require.Len(t, dep.Spec.Template.Spec.Containers, 1)
	assert.Equal(t, "bitnami/redis-exporter:latest", dep.Spec.Template.Spec.Containers[0].Image)
	require.Len(t, dep.Spec.Template.Spec.Containers[0].Ports, 1)
	assert.Equal(t, int32(9121), dep.Spec.Template.Spec.Containers[0].Ports[0].ContainerPort)
	require.Len(t, dep.OwnerReferences, 1)
	assert.Equal(t, redis.Name, dep.OwnerReferences[0].Name)
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
	var dep appsv1.Deployment
	require.NoError(t, r.Get(ctx, request().NamespacedName, &dep))
	assert.Equal(t, "redis:8.2", dep.Spec.Template.Spec.Containers[0].Image)
}

func TestReconcileRecreatesMissingChild(t *testing.T) {
	ctx := context.Background()
	r := newReconciler(t, instance())

	// sync
	_, err := r.Reconcile(ctx, request())
	require.NoError(t, err)
	var dep appsv1.Deployment
	require.NoError(t, r.Get(ctx, request().NamespacedName, &dep))

	// delete the deployment and reconcile
	require.NoError(t, r.Delete(ctx, &dep))
	_, err = r.Reconcile(ctx, request())
	require.NoError(t, err)

	// deployment was recreated
	require.NoError(t, r.Get(ctx, request().NamespacedName, &dep))
}

func TestReconcileMissingParentDoesNothing(t *testing.T) {
	// set up without parent
	ctx := context.Background()
	r := newReconciler(t)
	_, err := r.Reconcile(ctx, request())
	require.NoError(t, err)

	// no deployment should be created
	var dep appsv1.Deployment
	require.True(t, k8serrors.IsNotFound(r.Get(ctx, request().NamespacedName, &dep)))
}

func TestReconcileUpdatesStatus(t *testing.T) {
	tests := []struct {
		name     string
		exporter *string
		ready    []string
		want     cachev1alpha1.InstanceStatus
	}{
		{
			name: "redis not ready",
			want: cachev1alpha1.InstanceStatus{Message: "Waiting for Redis"},
		},
		{
			name:  "redis ready",
			ready: []string{"cache"},
			want:  cachev1alpha1.InstanceStatus{Ready: true, Message: "Redis instance is ready"},
		},
		{
			name:     "exporter not ready",
			exporter: new("bitnami/redis-exporter:latest"),
			ready:    []string{"cache"},
			want:     cachev1alpha1.InstanceStatus{Message: "Waiting for Redis exporter"},
		},
		{
			name:     "redis and exporter ready",
			exporter: new("bitnami/redis-exporter:latest"),
			ready:    []string{"cache", "cache-exporter"},
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

			// mark deployments as ready and reconcile
			for _, name := range tt.ready {
				var dep appsv1.Deployment
				require.NoError(t, r.Get(ctx, types.NamespacedName{Namespace: redis.Namespace, Name: name}, &dep))
				dep.Status = appsv1.DeploymentStatus{ObservedGeneration: dep.Generation, UpdatedReplicas: 1, AvailableReplicas: 1}
				require.NoError(t, r.Status().Update(ctx, &dep))
			}
			_, err = r.Reconcile(ctx, request())
			require.NoError(t, err)

			require.NoError(t, r.Get(ctx, request().NamespacedName, &current))
			assert.Equal(t, tt.want, current.Status)
		})
	}
}

func TestRedisInstanceReconciler_Reconcile_SwitchPersistence(t *testing.T) {
	ctx := context.Background()
	redis := instance()
	r := newReconciler(t, redis)

	// reconcile without persistence
	_, err := r.Reconcile(ctx, request())
	require.NoError(t, err)

	// after reconciliation, there should be a deployment and no statefulSet
	var dep appsv1.Deployment
	require.NoError(t, r.Get(ctx, request().NamespacedName, &dep))
	var sfs appsv1.StatefulSet
	require.Error(t, r.Get(ctx, request().NamespacedName, &sfs))

	// ask for persistence
	var updated cachev1alpha1.Instance
	require.NoError(t, r.Get(ctx, request().NamespacedName, &updated))
	updated.Spec.Persistence = &corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}
	require.NoError(t, r.Update(ctx, &updated))

	// first reconcile with persistence removes the deployment and re-queues
	result, err := r.Reconcile(ctx, request())
	require.NoError(t, err)
	require.NotZero(t, result.RequeueAfter)
	require.Error(t, r.Get(ctx, request().NamespacedName, &appsv1.Deployment{}))

	// next reconcile creates the statefulSet
	_, err = r.Reconcile(ctx, request())
	require.NoError(t, err)
	require.Error(t, r.Get(ctx, request().NamespacedName, &appsv1.Deployment{}))
	require.NoError(t, r.Get(ctx, request().NamespacedName, &sfs))

	// switch back to no persistence
	require.NoError(t, r.Get(ctx, request().NamespacedName, &updated))
	updated.Spec.Persistence = nil
	require.NoError(t, r.Update(ctx, &updated))

	// first reconcile without persistence removes the statefulSet and re-queues
	result, err = r.Reconcile(ctx, request())
	require.NoError(t, err)
	require.NotZero(t, result.RequeueAfter)
	require.Error(t, r.Get(ctx, request().NamespacedName, &appsv1.StatefulSet{}))

	// next reconcile creates the deployment
	_, err = r.Reconcile(ctx, request())
	require.NoError(t, err)
	require.NoError(t, r.Get(ctx, request().NamespacedName, &dep))
	require.Error(t, r.Get(ctx, request().NamespacedName, &sfs))

}

func TestInstanceStatus(t *testing.T) {
	depReady := &appsv1.Deployment{Status: appsv1.DeploymentStatus{UpdatedReplicas: 1, AvailableReplicas: 1}}
	depNotReady := &appsv1.Deployment{}
	depOutdated := &appsv1.Deployment{Status: appsv1.DeploymentStatus{UpdatedReplicas: 1, AvailableReplicas: 1}}
	depOutdated.Generation = 2
	depOutdated.Status.ObservedGeneration = 1
	sfsReady := &appsv1.StatefulSet{Status: appsv1.StatefulSetStatus{UpdatedReplicas: 1, AvailableReplicas: 1}}
	sfsNotReady := &appsv1.StatefulSet{}
	sfsOutdated := &appsv1.StatefulSet{Status: appsv1.StatefulSetStatus{UpdatedReplicas: 1, AvailableReplicas: 1}}
	sfsOutdated.Generation = 2
	sfsOutdated.Status.ObservedGeneration = 1

	tests := []struct {
		name string
		s    servers
		want cachev1alpha1.InstanceStatus
	}{
		{"redis deployment not ready", servers{server: depNotReady}, cachev1alpha1.InstanceStatus{Message: "Waiting for Redis"}},
		{"redis deployment outdated", servers{server: depOutdated}, cachev1alpha1.InstanceStatus{Message: "Waiting for Redis"}},
		{"redis deployment ready, no exporter", servers{server: depReady}, cachev1alpha1.InstanceStatus{Ready: true, Message: "Redis instance is ready"}},
		{"redis deployment not ready, exporter ready", servers{server: depNotReady, exporter: depReady}, cachev1alpha1.InstanceStatus{Message: "Waiting for Redis"}},
		{"redis deployment ready, exporter not ready", servers{server: depReady, exporter: depNotReady}, cachev1alpha1.InstanceStatus{Message: "Waiting for Redis exporter"}},
		{"redis deployment and exporter ready", servers{server: depReady, exporter: depReady}, cachev1alpha1.InstanceStatus{Ready: true, Message: "Redis instance is ready"}},
		{"redis statefulSet not ready", servers{server: sfsNotReady}, cachev1alpha1.InstanceStatus{Message: "Waiting for Redis"}},
		{"redis statefulSet outdated", servers{server: sfsOutdated}, cachev1alpha1.InstanceStatus{Message: "Waiting for Redis"}},
		{"redis statefulSet ready, no exporter", servers{server: sfsReady}, cachev1alpha1.InstanceStatus{Ready: true, Message: "Redis instance is ready"}},
		{"redis statefulSet not ready, exporter ready", servers{server: sfsNotReady, exporter: depReady}, cachev1alpha1.InstanceStatus{Message: "Waiting for Redis"}},
		{"redis statefulSet ready, exporter not ready", servers{server: sfsReady, exporter: depNotReady}, cachev1alpha1.InstanceStatus{Message: "Waiting for Redis exporter"}},
		{"redis statefulSet and exporter ready", servers{server: sfsReady, exporter: depReady}, cachev1alpha1.InstanceStatus{Ready: true, Message: "Redis instance is ready"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.s.Status())
		})
	}
}

func TestRedisDeployment(t *testing.T) {
	tests := []struct {
		name     string
		instance *cachev1alpha1.Instance
	}{
		{
			name: "simple deployment",
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
			dep := appsv1.Deployment{
				Name:      tt.instance.Name,
				Namespace: tt.instance.Namespace,
			}
			redisDeployment(&dep, tt.instance)
			marshalAndCompare(t, &dep)
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
