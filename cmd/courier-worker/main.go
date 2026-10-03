// Command courier-worker is the untrusted sandbox worker's signed-protocol
// listener (HARNESS.md §5). It verifies every signed envelope against the
// operator-provisioned public key and the exact run, control-incarnation, and
// worker-pod identity, executes the dispatched command in its sanitized
// workspace, and returns untrusted results. The listener is not a security
// boundary: isolation comes from the pod having no identity material and a
// network policy that admits only the control pod.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/misospace/courier/internal/protocol"
	"github.com/misospace/courier/internal/topology"
)

func main() {
	if err := runArgs(os.Args[1:]); err != nil {
		log.Fatal(err)
	}
}

func runArgs(args []string) error {
	flags := flag.NewFlagSet("courier-worker", flag.ContinueOnError)
	var addr, workspace string
	flags.StringVar(&addr, "addr", fmt.Sprint(protocol.WorkerPort), "listener port")
	flags.StringVar(&workspace, "workspace", "/workspace", "sanitized workspace directory")
	if err := flags.Parse(args); err != nil {
		return err
	}

	runUID := os.Getenv(topology.EnvRunUID)
	controlUID := os.Getenv(topology.EnvControlPodUID)
	workerUID := os.Getenv(topology.EnvWorkerPodUID)
	publicKeyB64 := os.Getenv(topology.EnvWorkerPublicKey)
	if runUID == "" || controlUID == "" || workerUID == "" || publicKeyB64 == "" {
		return errors.New("courier-worker requires run, control-incarnation, worker-pod identity, and the verification key")
	}
	publicKey, err := protocol.DecodeKey(publicKeyB64)
	if err != nil {
		return err
	}
	if info, err := os.Stat(workspace); err != nil || !info.IsDir() {
		return fmt.Errorf("courier-worker: workspace %s is unavailable", workspace)
	}

	worker, err := protocol.NewWorker(protocol.WorkerConfig{
		RunUID:        runUID,
		ControlPodUID: controlUID,
		WorkerPodUID:  workerUID,
		PublicKey:     publicKey,
		WorkspaceDir:  workspace,
	})
	if err != nil {
		return err
	}

	server := &http.Server{
		Addr:              ":" + addr,
		Handler:           worker.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	log.Printf("courier-worker listening on :%s", addr)
	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}

	// On pod termination, cancel every active operation and wait for verified
	// process termination before exiting.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	worker.Shutdown(shutdownCtx)
	return server.Shutdown(shutdownCtx)
}
