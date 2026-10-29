package main

import (
	"flag"
	"log/slog"
	"os"

	cachev1alpha1 "github.com/clambin/redis-controller/api/v1alpha1"
	rediscontroller "github.com/clambin/redis-controller/internal/controller"
	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
)

var (
	version = "change_me"
	debug   = flag.Bool("debug", false, "Enable debug logging")
)

func main() {
	var opts slog.HandlerOptions
	if *debug {
		opts.Level = slog.LevelDebug
	}
	ctrl.SetLogger(logr.FromSlogHandler(slog.NewJSONHandler(os.Stderr, &opts)))
	ctrl.Log.Info("starting redis-controller", "version", version)

	scheme := runtime.NewScheme()
	must(corev1.AddToScheme(scheme))
	must(appsv1.AddToScheme(scheme))
	must(cachev1alpha1.AddToScheme(scheme))

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{Scheme: scheme})
	must(err)

	reconciler := &rediscontroller.RedisInstanceReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme()}
	must(reconciler.SetupWithManager(mgr))
	must(mgr.Start(ctrl.SetupSignalHandler()))
}

func must(err error) {
	if err != nil {
		ctrl.Log.Error(err, "fatal error")
		os.Exit(1)
	}
}
