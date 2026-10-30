package main

import (
	"flag"
	"log/slog"
	"os"

	rediscontroller "github.com/clambin/redis-controller/internal/controller"
	"github.com/go-logr/logr"
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

	mgr, err := rediscontroller.NewManager(ctrl.GetConfigOrDie())
	if err != nil {
		ctrl.Log.Error(err, "failed to create manager", "err", err)
		os.Exit(1)
	}

	if err = mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		ctrl.Log.Error(err, "failed to start manager", "err", err)
		os.Exit(1)
	}
}
