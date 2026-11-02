package controller

import (
	"context"
	"fmt"
	"slices"
	"strconv"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
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

// RedisInstanceReconciler ensures that each RedisInstance has a Redis workload, a Service and,
// optionally, an exporter Deployment.
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

	var a artifacts
	result, err := r.reconcile(ctx, &redis, &a)
	if err != nil {
		log.Error(err, "failed to reconcile RedisInstance", "err", err)
		return result, err
	}
	if err = r.updateStatus(ctx, &redis, a); err != nil {
		log.Error(err, "failed to update RedisInstance status", "err", err)
		return reconcile.Result{}, err
	}
	return result, nil
}

// reconcile converges the child resources of the RedisInstance to the desired state.
func (r *RedisInstanceReconciler) reconcile(ctx context.Context, redis *cachev1alpha1.Instance, artifacts *artifacts) (reconcile.Result, error) {
	artifacts.redisServer = &appsv1.StatefulSet{Name: redis.Name, Namespace: redis.Namespace}
	if err := r.reconcileChild(ctx, redis, "redis statefulSet", artifacts.redisServer, func() { redisStatefulSet(artifacts.redisServer, redis) }); err != nil {
		return reconcile.Result{}, err
	}

	artifacts.service = &corev1.Service{Name: redis.Name, Namespace: redis.Namespace}
	if err := r.reconcileChild(ctx, redis, "service", artifacts.service, func() { redisService(artifacts.service, redis) }); err != nil {
		return reconcile.Result{}, err
	}

	var err error
	artifacts.exporter, err = r.reconcileExporter(ctx, redis)
	if err != nil {
		return reconcile.Result{}, err
	}
	return reconcile.Result{}, nil
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
func (r *RedisInstanceReconciler) updateStatus(ctx context.Context, redis *cachev1alpha1.Instance, a artifacts) error {
	status := a.instanceStatus()
	if slices.Equal(redis.Status.Conditions, status.Conditions) {
		return nil
	}
	before := redis.DeepCopy()
	previous := redis.Status.Conditions
	for _, cond := range status.Conditions {
		if old := meta.FindStatusCondition(previous, cond.Type); old != nil && old.Status == cond.Status {
			cond.LastTransitionTime = old.LastTransitionTime
		}
		meta.SetStatusCondition(&redis.Status.Conditions, cond)
	}
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
		ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
			Name: credentials.SecretName,
			Key:  credentials.PasswordKey,
		}},
	}}
}

func exporterName(redis *cachev1alpha1.Instance) string {
	return redis.Name + "-exporter"
}

func labels(name string) map[string]string {
	return map[string]string{appLabel: name}
}

type artifacts struct {
	redisServer *appsv1.StatefulSet
	service     *corev1.Service
	exporter    *appsv1.Deployment
}

func (a artifacts) instanceStatus() cachev1alpha1.InstanceStatus {
	var (
		instanceStatus cachev1alpha1.InstanceStatus
		status         metav1.ConditionStatus
		reason         string
		message        string
		instanceReady  = metav1.ConditionTrue
	)
	// Redis Server
	switch isReady(a.redisServer) {
	case true:
		status = metav1.ConditionTrue
		reason = "Available"
		message = "Redis server is ready"
	case false:
		instanceReady = metav1.ConditionFalse
		status = metav1.ConditionFalse
		reason = "Progressing"
		message = "Waiting for Redis server"
	}
	instanceStatus.Conditions = append(instanceStatus.Conditions, metav1.Condition{
		Type:    "RedisServer",
		Status:  status,
		Reason:  reason,
		Message: message,
	})
	// Redis Service
	switch {
	case a.service != nil:
		status = metav1.ConditionTrue
		reason = "Available"
		message = "Redis service is ready"
	default:
		instanceReady = metav1.ConditionFalse
		status = metav1.ConditionFalse
		reason = "Progressing"
		message = "Waiting for Redis service"
	}
	instanceStatus.Conditions = append(instanceStatus.Conditions, metav1.Condition{
		Type:    "RedisService",
		Status:  status,
		Reason:  reason,
		Message: message,
	})
	// Exporter
	if a.exporter != nil {
		switch isReady(a.exporter) {
		case true:
			status = metav1.ConditionTrue
			reason = "Available"
			message = "Exporter is ready"
		case false:
			instanceReady = metav1.ConditionFalse
			status = metav1.ConditionFalse
			reason = "Progressing"
			message = "Waiting for exporter"
		}
		instanceStatus.Conditions = append(instanceStatus.Conditions, metav1.Condition{
			Type:    "Exporter",
			Status:  status,
			Reason:  reason,
			Message: message,
		})
	}
	switch instanceReady {
	case metav1.ConditionTrue:
		reason = "Available"
		message = "All components are ready"
	case metav1.ConditionFalse:
		// TODO: why does Goland think this is unreachable?
		reason = "Progressing"
		message = "Waiting for child components"
	}
	instanceStatus.Conditions = append(instanceStatus.Conditions, metav1.Condition{
		Type:    "Ready",
		Status:  instanceReady,
		Reason:  reason,
		Message: message,
	})

	return instanceStatus
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
