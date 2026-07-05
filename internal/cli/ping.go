package cli

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/ChaosHour/Partition-Control-System/internal/db"
)

// cmdPing connects to the server and reports version and replica state.
// It doubles as the smoke test for the connection layer and flags.
func cmdPing(args []string) int {
	fs := flag.NewFlagSet("pcs ping", flag.ExitOnError)
	var conn db.ConnFlags
	conn.Register(fs)
	fs.Parse(args)

	ctx := context.Background()
	d, err := conn.Connect(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "pcs ping: %v\n", err)
		return 1
	}
	defer d.Close()

	replica, reason, err := d.IsReplica(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "pcs ping: %v\n", err)
		return 1
	}
	role := "writer"
	if replica {
		role = "replica"
	}
	fmt.Printf("server:  %s (parsed %s)\n", d.Version.Raw, d.Version)
	fmt.Printf("role:    %s — %s\n", role, reason)
	return 0
}
