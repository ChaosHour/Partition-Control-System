// pcs - Partition Control System
//
// Manages daily RANGE partitions (create, retain, drop) on MySQL tables
// with a timestamp or datetime column. Supports MySQL 5.7 through 9.x,
// including AWS RDS/Aurora, GCP Cloud SQL and Percona Server.
package main

import (
	"os"

	"github.com/ChaosHour/Partition-Control-System/internal/cli"
)

func main() {
	os.Exit(cli.Run(os.Args[1:]))
}
