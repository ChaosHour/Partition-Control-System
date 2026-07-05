// Package db provides the MySQL connection layer for pcs: connection
// flags, DSN handling, server version detection and the version-aware
// replica check. Compatible with MySQL 5.7 through 9.x, including AWS
// RDS/Aurora, GCP Cloud SQL and Percona Server.
package db

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/go-sql-driver/mysql"
)

// ConnFlags holds the connection settings shared by every subcommand.
// Values default from PCS_* environment variables so credentials can stay
// out of process listings and crontabs.
type ConnFlags struct {
	DSN      string
	Host     string
	Port     int
	Socket   string
	User     string
	Password string
	TLS      string
	Timeout  time.Duration

	// GCP Cloud SQL connector (alternative to host/port + Auth Proxy).
	CloudSQLInstance string
	IAMAuth          bool
}

// Register defines the shared connection flags on fs.
func (c *ConnFlags) Register(fs *flag.FlagSet) {
	fs.StringVar(&c.DSN, "dsn", os.Getenv("PCS_DSN"),
		"full go-sql-driver DSN; overrides all other connection flags (env PCS_DSN)")
	fs.StringVar(&c.Host, "host", envOr("PCS_HOST", "127.0.0.1"), "MySQL host (env PCS_HOST)")
	fs.IntVar(&c.Port, "port", 3306, "MySQL TCP port")
	fs.StringVar(&c.Socket, "socket", "", "unix socket path; overrides host/port")
	fs.StringVar(&c.User, "user", envOr("PCS_USER", "root"), "MySQL user (env PCS_USER)")
	fs.StringVar(&c.Password, "password", os.Getenv("PCS_PASSWORD"),
		"MySQL password; prefer env PCS_PASSWORD over this flag")
	fs.StringVar(&c.TLS, "tls", "", `TLS mode: "true", "preferred" or "skip-verify" (required for most RDS/Cloud SQL public endpoints)`)
	fs.DurationVar(&c.Timeout, "timeout", 10*time.Second, "connect timeout")
	fs.StringVar(&c.CloudSQLInstance, "cloudsql-instance", os.Getenv("PCS_CLOUDSQL_INSTANCE"),
		"GCP Cloud SQL instance connection name project:region:instance; dials via the Cloud SQL connector using Application Default Credentials (env PCS_CLOUDSQL_INSTANCE)")
	fs.BoolVar(&c.IAMAuth, "iam-auth", false,
		"use IAM database authentication with -cloudsql-instance (no DB password)")
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// DB wraps sql.DB with the detected server version.
type DB struct {
	*sql.DB
	Version Version
}

// Connect opens the connection described by the flags, verifies it with a
// ping and detects the server version.
func (c *ConnFlags) Connect(ctx context.Context) (*DB, error) {
	cfg, err := c.mysqlConfig()
	if err != nil {
		return nil, err
	}
	if c.CloudSQLInstance != "" {
		if err := registerCloudSQL(ctx, c.CloudSQLInstance, c.IAMAuth); err != nil {
			return nil, err
		}
		applyCloudSQL(cfg, c.CloudSQLInstance, c.IAMAuth)
	} else if c.IAMAuth {
		return nil, fmt.Errorf("-iam-auth requires -cloudsql-instance")
	}
	connector, err := mysql.NewConnector(cfg)
	if err != nil {
		return nil, fmt.Errorf("invalid connection config: %w", err)
	}
	sqlDB := sql.OpenDB(connector)
	// One managing connection is all pcs ever needs; DDL is serial.
	sqlDB.SetMaxOpenConns(2)

	if err := sqlDB.PingContext(ctx); err != nil {
		sqlDB.Close()
		return nil, fmt.Errorf("cannot connect to %s: %w", cfg.Addr, err)
	}

	var raw string
	if err := sqlDB.QueryRowContext(ctx, "SELECT VERSION()").Scan(&raw); err != nil {
		sqlDB.Close()
		return nil, fmt.Errorf("detecting server version: %w", err)
	}
	return &DB{DB: sqlDB, Version: ParseVersion(raw)}, nil
}

func (c *ConnFlags) mysqlConfig() (*mysql.Config, error) {
	if c.DSN != "" {
		cfg, err := mysql.ParseDSN(c.DSN)
		if err != nil {
			return nil, fmt.Errorf("invalid -dsn: %w", err)
		}
		return cfg, nil
	}
	cfg := mysql.NewConfig()
	cfg.User = c.User
	cfg.Passwd = c.Password
	if c.Socket != "" {
		cfg.Net = "unix"
		cfg.Addr = c.Socket
	} else {
		cfg.Net = "tcp"
		cfg.Addr = fmt.Sprintf("%s:%d", c.Host, c.Port)
	}
	cfg.Timeout = c.Timeout
	cfg.ReadTimeout = 5 * time.Minute // ALTERs on big tables are slow
	cfg.WriteTimeout = 1 * time.Minute
	if c.TLS != "" {
		cfg.TLSConfig = c.TLS
	}
	return cfg, nil
}
