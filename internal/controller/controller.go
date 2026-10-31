package controller

import (
	"context"
	"fmt"
	"strconv"

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

// RedisInstanceReconciler ensures that each RedisInstance has a Deployment and Service.
type RedisInstanceReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// Reconcile reads the RedisInstance and converges its child resources to the desired state.
func (r *RedisInstanceReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	log := ctrl.LoggerFrom(ctx).WithValues("instance", req.NamespacedName)
	ctx = ctrl.LoggerInto(ctx, log)
	log.Info("Reconciling RedisInstance")

	var redis cachev1alpha1.Instance
	if err := r.Get(ctx, req.NamespacedName, &redis); err != nil {
		// NotFound means the parent was deleted. Owner references let Kubernetes GC
		// remove its children, so there is nothing for this reconciler to do.
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}

	if redis.Spec.Image == "" {
		return reconcile.Result{}, fmt.Errorf("spec.image must not be empty")
	}

	// reconcile redis instance. if no persistence is specified, use deployment; otherwise use statefulSet
	var dep *appsv1.Deployment
	var sfs *appsv1.StatefulSet
	if redis.Spec.Persistence == nil {
		dep = &appsv1.Deployment{Name: redis.Name, Namespace: redis.Namespace}
		if err := r.reconcileChild(ctx, &redis, "deployment", dep, func() { redisDeployment(dep, &redis) }); err != nil {
			return reconcile.Result{}, err
		}
	} else {
		sfs = &appsv1.StatefulSet{Name: redis.Name, Namespace: redis.Namespace}
		if err := r.reconcileChild(ctx, &redis, "statefulset", sfs, func() { redisStatefulSet(sfs, &redis) }); err != nil {
			return reconcile.Result{}, err
		}
	}
	// reconcile service
	svc := &corev1.Service{Name: redis.Name, Namespace: redis.Namespace}
	if err := r.reconcileChild(ctx, &redis, "service", svc, func() { redisService(svc, &redis) }); err != nil {
		return reconcile.Result{}, err
	}
	// reconcile exporter
	var exporter *appsv1.Deployment
	if redis.Spec.Exporter != nil {
		exporter = &appsv1.Deployment{Name: exporterName(&redis), Namespace: redis.Namespace}
		if err := r.reconcileChild(ctx, &redis, "exporter deployment", exporter, func() { redisExporterDeployment(exporter, &redis) }); err != nil {
			return reconcile.Result{}, err
		}
	}

	return reconcile.Result{}, r.updateStatus(ctx, &redis, instanceStatus(dep, sfs, exporter))
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
func (r *RedisInstanceReconciler) updateStatus(ctx context.Context, redis *cachev1alpha1.Instance, status cachev1alpha1.Status) error {
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

// instanceStatus determines the status of a RedisInstance from its deployments. exporter is nil if no exporter is configured.
func instanceStatus(dep *appsv1.Deployment, sfs *appsv1.StatefulSet, exporter *appsv1.Deployment) cachev1alpha1.Status {
	switch {
	case dep != nil && !deploymentReady(dep):
		return cachev1alpha1.Status{Ready: false, Message: "Waiting for Redis"}
	case sfs != nil && !statefulSetReady(sfs):
		return cachev1alpha1.Status{Ready: false, Message: "Waiting for Redis"}
	case exporter != nil && !deploymentReady(exporter):
		return cachev1alpha1.Status{Ready: false, Message: "Waiting for Redis exporter"}
	default:
		return cachev1alpha1.Status{Ready: true, Message: "Redis instance is ready"}
	}
}

func deploymentReady(dep *appsv1.Deployment) bool {
	const desired = 1
	return dep.Status.ObservedGeneration >= dep.Generation &&
		dep.Status.UpdatedReplicas >= desired &&
		dep.Status.AvailableReplicas >= desired
}

func statefulSetReady(sfs *appsv1.StatefulSet) bool {
	const desired = 1
	return sfs.Status.ObservedGeneration >= sfs.Generation &&
		sfs.Status.UpdatedReplicas >= desired &&
		sfs.Status.AvailableReplicas >= desired
}

func redisDeployment(dep *appsv1.Deployment, redis *cachev1alpha1.Instance) {
	dep.Labels = labels(redis.Name)
	dep.Spec.Replicas = new(int32(1))
	dep.Spec.Selector = &metav1.LabelSelector{MatchLabels: labels(redis.Name)}
	dep.Spec.Template.Labels = labels(redis.Name)
	dep.Spec.Template.Spec.RestartPolicy = corev1.RestartPolicyAlways
	dep.Spec.Template.Spec.Containers = []corev1.Container{redisContainer(redis)}
	// if persistence is required, mount the volume as per the crd's configuration
	if redis.Spec.Persistence != nil {
		dep.Spec.Template.Spec.Volumes = []corev1.Volume{{Name: "redis-data", VolumeSource: *redis.Spec.Persistence}}
	}
}

func redisStatefulSet(sfs *appsv1.StatefulSet, redis *cachev1alpha1.Instance) {
	sfs.Labels = labels(redis.Name)
	sfs.Spec.Replicas = new(int32(1))
	sfs.Spec.Selector = &metav1.LabelSelector{MatchLabels: labels(redis.Name)}
	sfs.Spec.Template.Labels = labels(redis.Name)
	sfs.Spec.Template.Spec.RestartPolicy = corev1.RestartPolicyAlways
	sfs.Spec.Template.Spec.Containers = []corev1.Container{redisContainer(redis)}
	// if persistence is required, mount the volume as per the crd's configuration
	if redis.Spec.Persistence != nil {
		sfs.Spec.Template.Spec.Volumes = []corev1.Volume{{Name: "redis-data", VolumeSource: *redis.Spec.Persistence}}
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
func passwordEnv(credentials *cachev1alpha1.Credentials) []corev1.EnvVar {
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
