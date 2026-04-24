package bunshin

import (
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"
)

func TestTLSConfigFromFiles(t *testing.T) {
	cert, err := generateSelfSignedCert()
	if err != nil {
		t.Fatal(err)
	}
	if len(cert.Certificate) == 0 {
		t.Fatal("generated certificate has no DER data")
	}

	dir := t.TempDir()
	certFile := filepath.Join(dir, "server.crt")
	keyFile := filepath.Join(dir, "server.key")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Certificate[0]}), 0o600); err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(cert.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}

	serverTLS, err := ServerTLSConfigFromFiles(ServerTLSFiles{
		CertFile: certFile,
		KeyFile:  keyFile,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(serverTLS.Certificates) != 1 {
		t.Fatalf("server certificates = %d, want 1", len(serverTLS.Certificates))
	}

	clientTLS, err := ClientTLSConfigFromFiles(ClientTLSFiles{
		CAFile:     certFile,
		ServerName: "localhost",
	})
	if err != nil {
		t.Fatal(err)
	}
	if clientTLS.RootCAs == nil {
		t.Fatal("client root CAs not configured")
	}
}
