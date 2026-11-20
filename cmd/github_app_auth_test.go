package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestLoadGitHubAppAuth(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	encoded := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	reader := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "app-key", Namespace: "courier-system"},
		Data:       map[string][]byte{"pem": encoded},
	}).Build()
	if auth, err := loadGitHubAppAuth(context.Background(), reader, "courier-system", 0, 0, "", "", "", "https://api.github.com/"); err != nil || auth != nil {
		t.Fatalf("disabled auth = %#v, %v", auth, err)
	}
	auth, err := loadGitHubAppAuth(context.Background(), reader, "courier-system", 123, 456, "acme", "app-key", "pem", "https://api.github.com/")
	if err != nil {
		t.Fatal(err)
	}
	if auth.AppID != 123 || auth.InstallationID != 456 || auth.Owner != "acme" || auth.PrivateKey == nil {
		t.Fatalf("loaded GitHub App auth has unexpected IDs or no private key")
	}
	if _, err := loadGitHubAppAuth(context.Background(), reader, "courier-system", 123, 0, "acme", "app-key", "pem", "https://api.github.com/"); err == nil {
		t.Fatal("partial GitHub App configuration must fail")
	}
}
