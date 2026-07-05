package db

import (
	"context"
	"fmt"
	"net"

	"cloud.google.com/go/cloudsqlconn"
	"github.com/go-sql-driver/mysql"
)

// registerCloudSQL wires the Cloud SQL Go connector into the mysql
// driver as a custom network named "cloudsql". The dialer authenticates
// with Application Default Credentials (gcloud auth application-default
// login, workload identity, or GOOGLE_APPLICATION_CREDENTIALS) and
// establishes mTLS to the instance — no Auth Proxy process and no
// authorized-network/public-IP exposure needed.
//
// With iamAuth, the connector also handles IAM database authentication:
// connect as the IAM user (e.g. -user sa-name for
// sa-name@project.iam.gserviceaccount.com) with no password.
func registerCloudSQL(ctx context.Context, instance string, iamAuth bool) error {
	var opts []cloudsqlconn.Option
	if iamAuth {
		opts = append(opts, cloudsqlconn.WithIAMAuthN())
	}
	d, err := cloudsqlconn.NewDialer(ctx, opts...)
	if err != nil {
		return fmt.Errorf("creating Cloud SQL dialer: %w", err)
	}
	mysql.RegisterDialContext("cloudsql", func(ctx context.Context, _ string) (net.Conn, error) {
		return d.Dial(ctx, instance)
	})
	return nil
}

// applyCloudSQL adjusts a driver config to dial through the Cloud SQL
// connector instead of TCP.
func applyCloudSQL(cfg *mysql.Config, instance string, iamAuth bool) {
	cfg.Net = "cloudsql"
	cfg.Addr = instance
	cfg.TLSConfig = "" // the connector provides mTLS itself
	if iamAuth {
		// IAM authentication sends the OAuth2 token as the password
		// over the connector's TLS channel.
		cfg.AllowCleartextPasswords = true
	}
}
