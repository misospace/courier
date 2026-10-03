package topology

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"time"
)

// BrokerTLS is the per-run broker transport identity: a dedicated CA whose
// private key is destroyed immediately after issuance (control receives only
// the CA certificate as its trust anchor) and a server certificate for the
// run's broker Service DNS names.
//
// The certificate lives exactly as long as the run: runs have no wall-clock
// deadline by design, so the validity window is deliberately long and the
// identity is garbage-collected with the run's objects. Its blast radius is
// one run's broker pod.
type BrokerTLS struct {
	CACertPEM     []byte
	ServerCertPEM []byte
	ServerKeyPEM  []byte
}

// GenerateBrokerTLS issues the per-run CA and server certificate. dnsNames
// must include every name control may use to reach the broker (the Service's
// short and cluster-qualified DNS forms).
func GenerateBrokerTLS(dnsNames []string) (*BrokerTLS, error) {
	if len(dnsNames) == 0 {
		return nil, errors.New("topology: broker TLS requires at least one DNS name")
	}
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("topology: generate CA key: %w", err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          randomSerial(),
		Subject:               pkix.Name{CommonName: "courier-run-broker-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().AddDate(10, 0, 0),
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		return nil, fmt.Errorf("topology: create CA certificate: %w", err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		return nil, fmt.Errorf("topology: parse CA certificate: %w", err)
	}

	serverKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("topology: generate server key: %w", err)
	}
	serverTemplate := &x509.Certificate{
		SerialNumber: randomSerial(),
		Subject:      pkix.Name{CommonName: "courier-run-broker"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().AddDate(10, 0, 0),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     dnsNames,
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	serverDER, err := x509.CreateCertificate(rand.Reader, serverTemplate, caCert, &serverKey.PublicKey, caKey)
	if err != nil {
		return nil, fmt.Errorf("topology: create server certificate: %w", err)
	}

	// The CA private key is intentionally not returned or persisted: nothing
	// signs again for this run, and a leaked CA key would be usable only
	// against this run's name set.
	out := &BrokerTLS{
		CACertPEM:     pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}),
		ServerCertPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverDER}),
	}
	keyDER, err := x509.MarshalECPrivateKey(serverKey)
	if err != nil {
		return nil, fmt.Errorf("topology: marshal server key: %w", err)
	}
	out.ServerKeyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	return out, nil
}

func randomSerial() *big.Int {
	limit := new(big.Int).Lsh(big.NewInt(1), 128)
	serial, err := rand.Int(rand.Reader, limit)
	if err != nil {
		return big.NewInt(1)
	}
	return serial
}
