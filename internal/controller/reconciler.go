package controller

import (
	"context"
	"fmt"
	"strconv"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	cachev1alpha1 "github.com/clambin/redis-controller/api/v1alpha1"
)

const (
	appLabel          = "app.kubernetes.io/name"
	redisPort         = 6379
	redisExporterPort = 9121
	passwordEnvVar    = "REDIS_PASSWORD"
)

// RedisInstanceReconciler ensures that each RedisInstance has a Redis workload (a Deployment, or a StatefulSet
// when persistence is requested), a Service and, optionally, an exporter Deployment.
type RedisInstanceReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// Reconcile reads the RedisInstance and converges its child resources to the desired state.
func (r *RedisInstanceReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	var redis cachev1alpha1.Instance
	if err := r.Get(ctx, req.NamespacedName, &redis); err != nil {
		// NotFound means the parent was deleted. Owner references let Kubernetes GC
		// remove its children, so there is nothing for this reconciler to do.
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}

	log := ctrl.LoggerFrom(ctx).WithValues("instance", req.NamespacedName)
	ctx = ctrl.LoggerInto(ctx, log)
	log.Info("Reconciling RedisInstance")

	result, srvrs, err := r.reconcile(ctx, &redis)
	if err != nil {
		log.Error(err, "failed to reconcile RedisInstance", "err", err)
		return result, err
	}
	return result, r.updateStatus(ctx, &redis, srvrs.Status())
}

// reconcile converges the child resources of the RedisInstance to the desired state.
func (r *RedisInstanceReconciler) reconcile(ctx context.Context, redis *cachev1alpha1.Instance) (reconcile.Result, servers, error) {
	result, server, err := r.reconcileRedisServer(ctx, redis)
	if err != nil || !result.IsZero() {
		return result, servers{}, err
	}

	svc := &corev1.Service{Name: redis.Name, Namespace: redis.Namespace}
	if err = r.reconcileChild(ctx, redis, "service", svc, func() { redisService(svc, redis) }); err != nil {
		return reconcile.Result{}, servers{}, err
	}

	exporter, err := r.reconcileExporter(ctx, redis)
	if err != nil {
		return reconcile.Result{}, servers{}, err
	}
	return reconcile.Result{}, servers{server: server, exporter: exporter}, nil
}

// reconcileRedisServer runs redis as a Deployment, or as a StatefulSet when persistence is requested.
// Any workload of the other kind is removed first; while that is in progress, reconciliation is re-queued.
func (r *RedisInstanceReconciler) reconcileRedisServer(ctx context.Context, redis *cachev1alpha1.Instance) (reconcile.Result, client.Object, error) {
	var server, obsolete client.Object
	var mutate func()
	var kind string

	if redis.Spec.Persistence == nil {
		dep := &appsv1.Deployment{Name: redis.Name, Namespace: redis.Namespace}
		server, mutate, kind = dep, func() { redisDeployment(dep, redis) }, "redis deployment"
		obsolete = &appsv1.StatefulSet{Name: redis.Name, Namespace: redis.Namespace}
	} else {
		sfs := &appsv1.StatefulSet{Name: redis.Name, Namespace: redis.Namespace}
		server, mutate, kind = sfs, func() { redisStatefulSet(sfs, redis) }, "redis statefulSet"
		obsolete = &appsv1.Deployment{Name: redis.Name, Namespace: redis.Namespace}
	}

	if gone, err := r.removeObsolete(ctx, redis, obsolete); err != nil || !gone {
		if err != nil {
			return reconcile.Result{}, nil, err
		}
		return reconcile.Result{RequeueAfter: 5 * time.Second}, nil, nil
	}

	if err := r.reconcileChild(ctx, redis, kind, server, mutate); err != nil {
		return reconcile.Result{}, nil, err
	}
	return reconcile.Result{}, server, nil
}

// reconcileExporter creates or patches the exporter deployment. If no exporter is requested, any existing one is removed.
func (r *RedisInstanceReconciler) reconcileExporter(ctx context.Context, redis *cachev1alpha1.Instance) (*appsv1.Deployment, error) {
	exporter := &appsv1.Deployment{Name: exporterName(redis), Namespace: redis.Namespace}
	if redis.Spec.Exporter == nil {
		_, err := r.removeObsolete(ctx, redis, exporter)
		return nil, err
	}
	if err := r.reconcileChild(ctx, redis, "exporter deployment", exporter, func() { redisExporterDeployment(exporter, redis) }); err != nil {
		return nil, err
	}
	return exporter, nil
}

// removeObsolete deletes the child of the other workload kind. It reports true only when that child is gone.
func (r *RedisInstanceReconciler) removeObsolete(ctx context.Context, redis *cachev1alpha1.Instance, obj client.Object) (bool, error) {
	key := client.ObjectKeyFromObject(obj)
	if err := r.Get(ctx, key, obj); err != nil {
		return true, client.IgnoreNotFound(err)
	}
	if !metav1.IsControlledBy(obj, redis) {
		return false, fmt.Errorf("%T %s is not controlled by this instance", obj, key)
	}
	if obj.GetDeletionTimestamp() == nil {
		if err := r.Delete(ctx, obj, client.PropagationPolicy(metav1.DeletePropagationForeground)); client.IgnoreNotFound(err) != nil {
			return false, err
		}
	}
	return false, nil
}

// reconcileChild creates or patches a child resource of the RedisInstance. mutate sets the desired state on obj.
func (r *RedisInstanceReconciler) reconcileChild(ctx context.Context, redis *cachev1alpha1.Instance, kind string, obj client.Object, mutate func()) error {
	log := ctrl.LoggerFrom(ctx).WithValues("kind", kind, "name", obj.GetName())
	op, err := controllerutil.CreateOrPatch(ctx, r.Client, obj, func() error {
		mutate()
		return controllerutil.SetControllerReference(redis, obj, r.Scheme)
	})
	if err != nil {
		return fmt.Errorf("%s: %w", kind, err)
	}
	if op != controllerutil.OperationResultNone {
		log.Info("Child resource reconciled", "operation", op)
	} else {
		log.V(4).Info("Child resource already up to date")
	}
	return nil
}

// updateStatus patches the status of the RedisInstance, if it changed.
func (r *RedisInstanceReconciler) updateStatus(ctx context.Context, redis *cachev1alpha1.Instance, status cachev1alpha1.InstanceStatus) error {
	if redis.Status == status {
		return nil
	}
	before := redis.DeepCopy()
	redis.Status = status
	if err := r.Status().Patch(ctx, redis, client.MergeFrom(before)); err != nil {
		return fmt.Errorf("status: %w", err)
	}
	return nil
}

// SetupWithManager registers watches for the parent and its owned resources.
func (r *RedisInstanceReconciler) SetupWithManager(mgr manager.Manager) error {
	return builder.ControllerManagedBy(mgr).
		For(&cachev1alpha1.Instance{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Complete(r)
}

func redisDeployment(dep *appsv1.Deployment, redis *cachev1alpha1.Instance) {
	dep.Labels = labels(redis.Name)
	dep.Spec.Replicas = new(int32(1))
	dep.Spec.Selector = &metav1.LabelSelector{MatchLabels: labels(redis.Name)}
	redisPodTemplate(&dep.Spec.Template, redis)
}

func redisStatefulSet(sfs *appsv1.StatefulSet, redis *cachev1alpha1.Instance) {
	sfs.Labels = labels(redis.Name)
	sfs.Spec.Replicas = new(int32(1))
	sfs.Spec.Selector = &metav1.LabelSelector{MatchLabels: labels(redis.Name)}
	redisPodTemplate(&sfs.Spec.Template, redis)
}

// redisPodTemplate sets the desired state on the pod template in place, so server-side defaults are preserved.
func redisPodTemplate(tpl *corev1.PodTemplateSpec, redis *cachev1alpha1.Instance) {
	tpl.Labels = labels(redis.Name)
	tpl.Spec.RestartPolicy = corev1.RestartPolicyAlways
	tpl.Spec.Containers = []corev1.Container{redisContainer(redis)}
	// if persistence is required, mount the volume as per the crd's configuration
	if redis.Spec.Persistence != nil {
		tpl.Spec.Volumes = []corev1.Volume{{Name: "redis-data", VolumeSource: *redis.Spec.Persistence}}
	}
}

func redisContainer(redis *cachev1alpha1.Instance) corev1.Container {
	container := corev1.Container{
		Name:                     "redis",
		Image:                    redis.Spec.Image,
		ImagePullPolicy:          corev1.PullIfNotPresent,
		TerminationMessagePath:   corev1.TerminationMessagePathDefault,
		TerminationMessagePolicy: corev1.TerminationMessageReadFile,
		Ports: []corev1.ContainerPort{
			{Name: "redis", ContainerPort: redisPort, Protocol: corev1.ProtocolTCP},
		},
	}

	// depending on persistence, set up the args & volume mounts
	if redis.Spec.Persistence == nil {
		container.Args = []string{"--save", "--appendonly", "no"}
	} else {
		container.Args = []string{"--appendonly", "yes", "--appendfsync", "everysec"}
		container.VolumeMounts = []corev1.VolumeMount{
			{Name: "redis-data", MountPath: "/data"},
		}
	}

	// if credentials are required, set up the env and update the args
	if redis.Spec.Credentials != nil {
		container.Env = passwordEnv(redis.Spec.Credentials)
		container.Args = append(container.Args, "--requirepass", "$("+passwordEnvVar+")")
	}
	return container
}

func redisService(svc *corev1.Service, redis *cachev1alpha1.Instance) {
	svc.Labels = labels(redis.Name)
	svc.Spec.Selector = labels(redis.Name)
	svc.Spec.Type = corev1.ServiceTypeClusterIP
	svc.Spec.Ports = []corev1.ServicePort{
		{Name: "tcp-redis", Port: redisPort, TargetPort: intstr.IntOrString{IntVal: redisPort}, Protocol: corev1.ProtocolTCP},
	}
}

func redisExporterDeployment(dep *appsv1.Deployment, redis *cachev1alpha1.Instance) {
	container := corev1.Container{
		Name:                     "redis-exporter",
		Image:                    *redis.Spec.Exporter,
		ImagePullPolicy:          corev1.PullAlways,
		TerminationMessagePath:   corev1.TerminationMessagePathDefault,
		TerminationMessagePolicy: corev1.TerminationMessageReadFile,
		Ports: []corev1.ContainerPort{
			{Name: "metrics", ContainerPort: redisExporterPort, Protocol: corev1.ProtocolTCP},
		},
		Args: []string{"-redis.addr=redis://" + redis.Name + ":" + strconv.Itoa(redisPort)},
	}

	// if credentials are required, set up the env and update the args
	if redis.Spec.Credentials != nil {
		container.Env = passwordEnv(redis.Spec.Credentials)
		container.Args = append(container.Args, "-redis.password=$("+passwordEnvVar+")")
	}

	dep.Labels = labels(exporterName(redis))
	dep.Spec.Replicas = new(int32(1))
	dep.Spec.Selector = &metav1.LabelSelector{MatchLabels: labels(exporterName(redis))}
	dep.Spec.Template.Labels = labels(exporterName(redis))
	if dep.Spec.Template.Annotations == nil {
		dep.Spec.Template.Annotations = make(map[string]string)
	}
	dep.Spec.Template.Annotations["prometheus.io/scrape"] = "true"
	dep.Spec.Template.Annotations["prometheus.io/port"] = strconv.Itoa(redisExporterPort)
	dep.Spec.Template.Spec.Containers = []corev1.Container{container}
}

// passwordEnv exposes the Redis password from the credentials Secret as an environment variable.
func passwordEnv(credentials *cachev1alpha1.RedisCredentials) []corev1.EnvVar {
	return []corev1.EnvVar{{
		Name: passwordEnvVar,
		ValueFrom: &corev1.EnvVarSource{
			SecretKeyRef: &corev1.SecretKeySelector{
				Name: credentials.SecretName,
				Key:  credentials.PasswordKey,
			},
		},
	}}
}

func exporterName(redis *cachev1alpha1.Instance) string {
	return redis.Name + "-exporter"
}

func labels(name string) map[string]string {
	return map[string]string{appLabel: name}
}

type servers struct {
	server   client.Object
	exporter *appsv1.Deployment
}

func (s servers) Status() cachev1alpha1.InstanceStatus {
	switch {
	case !isReady(s.server):
		return cachev1alpha1.InstanceStatus{Ready: false, Message: "Waiting for Redis"}
	case s.exporter != nil && !isReady(s.exporter):
		return cachev1alpha1.InstanceStatus{Ready: false, Message: "Waiting for Redis exporter"}
	default:
		return cachev1alpha1.InstanceStatus{Ready: true, Message: "Redis instance is ready"}
	}
}

func isReady(srv client.Object) bool {
	if srv == nil {
		return false
	}
	const desired = 1
	switch srv := srv.(type) {
	case *appsv1.Deployment:
		return srv.Status.ObservedGeneration >= srv.Generation &&
			srv.Status.UpdatedReplicas >= desired &&
			srv.Status.AvailableReplicas >= desired &&
			srv.Status.UnavailableReplicas == 0
	case *appsv1.StatefulSet:
		return srv.Status.ObservedGeneration >= srv.Generation &&
			srv.Status.UpdatedReplicas >= desired &&
			srv.Status.AvailableReplicas >= desired
	default:
		panic(fmt.Errorf("unknown object type %T", srv))
	}
}
