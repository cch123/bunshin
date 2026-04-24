package bunshin

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

type ClientTLSFiles struct {
	CAFile     string
	CertFile   string
	KeyFile    string
	ServerName string
}

type ServerTLSFiles struct {
	CertFile string
	KeyFile  string
	ClientCA string
}

func ClientTLSConfigFromFiles(files ClientTLSFiles) (*tls.Config, error) {
	if files.CAFile == "" {
		return nil, fmt.Errorf("client CA file is required")
	}
	if files.ServerName == "" {
		return nil, fmt.Errorf("server name is required")
	}

	caPEM, err := os.ReadFile(files.CAFile)
	if err != nil {
		return nil, fmt.Errorf("read client CA file: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("parse client CA file: no certificates found")
	}

	cfg := &tls.Config{
		RootCAs:    roots,
		ServerName: files.ServerName,
		NextProtos: []string{quicALPN},
		MinVersion: tls.VersionTLS13,
	}
	if files.CertFile != "" || files.KeyFile != "" {
		if files.CertFile == "" || files.KeyFile == "" {
			return nil, fmt.Errorf("both client cert file and key file are required for mutual TLS")
		}
		cert, err := tls.LoadX509KeyPair(files.CertFile, files.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load client certificate: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

func ServerTLSConfigFromFiles(files ServerTLSFiles) (*tls.Config, error) {
	if files.CertFile == "" {
		return nil, fmt.Errorf("server cert file is required")
	}
	if files.KeyFile == "" {
		return nil, fmt.Errorf("server key file is required")
	}

	cert, err := tls.LoadX509KeyPair(files.CertFile, files.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load server certificate: %w", err)
	}

	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{quicALPN},
		MinVersion:   tls.VersionTLS13,
	}
	if files.ClientCA != "" {
		caPEM, err := os.ReadFile(files.ClientCA)
		if err != nil {
			return nil, fmt.Errorf("read client CA file: %w", err)
		}
		clientCAs := x509.NewCertPool()
		if !clientCAs.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("parse client CA file: no certificates found")
		}
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
		cfg.ClientCAs = clientCAs
	}
	return cfg, nil
}
