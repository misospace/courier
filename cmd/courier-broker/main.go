// Command courier-broker serves the run-scoped typed broker API.
// It remains unavailable until the operator (#123) provides resolved policy,
// Kubernetes identity clients, and per-run TLS material.
package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
)

func main() {
	if err := runArgs(os.Args[1:]); err != nil {
		log.Fatal(err)
	}
}

func runArgs(args []string) error {
	flags := flag.NewFlagSet("courier-broker", flag.ContinueOnError)
	var addr, certFile, keyFile, policyFile string
	flags.StringVar(&addr, "addr", ":8443", "TLS listener address")
	flags.StringVar(&certFile, "tls-cert", "", "per-run broker TLS certificate")
	flags.StringVar(&keyFile, "tls-key", "", "per-run broker TLS private key")
	flags.StringVar(&policyFile, "policy", "", "operator-resolved immutable run policy")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if addr == "" || certFile == "" || keyFile == "" || policyFile == "" {
		return errors.New("courier-broker requires address, TLS certificate/key, and run-bound policy; operator wiring is not implemented")
	}
	return fmt.Errorf("courier-broker cannot start: #123 operator wiring must provide run-bound policy and live TokenReview identity backend")
}
