package db

import (
	"flag"
	"os"
	"path/filepath"
	"testing"
)

func writeFile(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestParseMyCnf(t *testing.T) {
	path := writeFile(t, "my.cnf", `
# comment
[mysqld]
port = 3307

[client]
user = cnfuser
password = "p#ss word"
host=10.1.2.3
port = 3316   # trailing comment
no-beep
!includedir /etc/mysql.d
`)
	vals, err := parseMyCnf(path, "")
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"user": "cnfuser", "password": "p#ss word", "host": "10.1.2.3", "port": "3316"}
	for k, w := range want {
		if vals[k] != w {
			t.Errorf("vals[%q] = %q, want %q", k, vals[k], w)
		}
	}
	if _, ok := vals["port "]; ok {
		t.Error("keys not trimmed")
	}
}

func TestParseMyCnfGroupSuffix(t *testing.T) {
	path := writeFile(t, "my.cnf", `
[client]
user = base
password = pw
ssl-mode = REQUIRED

[client_primary1]
host = primary.example.com
port = 3307

[client_replica1]
host = replica.example.com
`)
	vals, err := parseMyCnf(path, "_primary1")
	if err != nil {
		t.Fatal(err)
	}
	if vals["user"] != "base" || vals["host"] != "primary.example.com" || vals["port"] != "3307" {
		t.Errorf("suffix merge: %v", vals)
	}
	if vals["host"] == "replica.example.com" {
		t.Error("wrong suffix group read")
	}
	// No suffix: [client] only.
	vals, err = parseMyCnf(path, "")
	if err != nil {
		t.Fatal(err)
	}
	if vals["host"] != "" {
		t.Errorf("no-suffix read host %q from suffix group", vals["host"])
	}
}

func TestTLSFromSSLMode(t *testing.T) {
	for mode, want := range map[string]string{
		"REQUIRED": "skip-verify", "PREFERRED": "preferred",
		"VERIFY_CA": "true", "VERIFY_IDENTITY": "true",
		"DISABLED": "", "": "",
	} {
		if got := tlsFromSSLMode(mode); got != want {
			t.Errorf("tlsFromSSLMode(%q) = %q, want %q", mode, got, want)
		}
	}
}

// newConn registers flags, parses args, resolves, and returns the result.
func newConn(t *testing.T, defaultsFile, credsFile string, args ...string) *ConnFlags {
	t.Helper()
	var c ConnFlags
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	c.Register(fs)
	if defaultsFile != "" {
		args = append(args, "-defaults-file", defaultsFile)
	}
	if credsFile != "" {
		args = append(args, "-credentials", credsFile)
	}
	if err := fs.Parse(args); err != nil {
		t.Fatal(err)
	}
	if err := c.resolve(); err != nil {
		t.Fatal(err)
	}
	return &c
}

const testCnf = "[client]\nuser = cnfuser\npassword = cnfpass\nhost = cnfhost\nport = 3316\n"
const testJSON = `{"user": "jsonuser", "password": "jsonpass", "port": 3326}`

func TestResolvePrecedence(t *testing.T) {
	t.Setenv("HOME", t.TempDir()) // keep the developer's ~/.my.cnf out
	cnf := writeFile(t, "my.cnf", testCnf)
	creds := writeFile(t, "creds.json", testJSON)

	// my.cnf alone fills everything unset.
	c := newConn(t, cnf, "")
	if c.User != "cnfuser" || c.Password != "cnfpass" || c.Host != "cnfhost" || c.Port != 3316 {
		t.Errorf("my.cnf alone: %+v", c)
	}

	// JSON overlays my.cnf; my.cnf still supplies host.
	c = newConn(t, cnf, creds)
	if c.User != "jsonuser" || c.Password != "jsonpass" || c.Port != 3326 || c.Host != "cnfhost" {
		t.Errorf("json over my.cnf: %+v", c)
	}

	// Explicit flags beat both files.
	c = newConn(t, cnf, creds, "-user", "flaguser", "-port", "3336")
	if c.User != "flaguser" || c.Port != 3336 || c.Password != "jsonpass" {
		t.Errorf("flags over files: %+v", c)
	}

	// Environment beats files but not flags.
	t.Setenv("PCS_PASSWORD", "envpass")
	t.Setenv("PCS_USER", "envuser")
	c = newConn(t, cnf, creds, "-user", "flaguser")
	if c.Password != "envpass" || c.User != "flaguser" {
		t.Errorf("env over files, flag over env: user=%q password=%q", c.User, c.Password)
	}
}

func TestResolveMissingFiles(t *testing.T) {
	t.Setenv("HOME", t.TempDir()) // keep the developer's ~/.my.cnf out

	// Explicit -defaults-file that doesn't exist is an error.
	var c ConnFlags
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	c.Register(fs)
	fs.Parse([]string{"-defaults-file", "/nonexistent/my.cnf"})
	if err := c.resolve(); err == nil {
		t.Error("missing explicit -defaults-file should error")
	}

	// Explicit -credentials that doesn't exist is an error.
	var c2 ConnFlags
	fs2 := flag.NewFlagSet("test", flag.ContinueOnError)
	c2.Register(fs2)
	fs2.Parse([]string{"-credentials", "/nonexistent/creds.json"})
	if err := c2.resolve(); err == nil {
		t.Error("missing explicit -credentials should error")
	}
}
