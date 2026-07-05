package db

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Connection settings resolve in this order, per field:
//
//	1. explicit command-line flag
//	2. PCS_* environment variable
//	3. JSON credentials file (-credentials / PCS_CREDENTIALS)
//	4. MySQL options file [client] section (-defaults-file, or ~/.my.cnf
//	   when present)
//	5. built-in default
//
// Fields can mix sources freely: -host on the command line with the
// password coming from ~/.my.cnf works.

// credFile is the JSON credentials file format. All fields optional.
type credFile struct {
	Host             string `json:"host"`
	Port             int    `json:"port"`
	Socket           string `json:"socket"`
	User             string `json:"user"`
	Password         string `json:"password"`
	TLS              string `json:"tls"`
	DSN              string `json:"dsn"`
	CloudSQLInstance string `json:"cloudsql_instance"`
}

// resolve fills unset connection fields from the JSON credentials file
// and the MySQL options file. Called by Connect after flag parsing.
func (c *ConnFlags) resolve() error {
	if c.fs == nil {
		return nil
	}
	set := map[string]bool{}
	c.fs.Visit(func(f *flag.Flag) { set[f.Name] = true })

	// A field is pinned when its flag was given or its env var is set
	// (env values were already loaded as flag defaults by Register).
	pinned := func(flagName, envName string) bool {
		if set[flagName] {
			return true
		}
		return envName != "" && os.Getenv(envName) != ""
	}

	// Merge file sources lowest-precedence first: the MySQL options
	// file provides the base, the JSON credentials file overlays it.
	merged := map[string]string{}

	path, required := c.DefaultsFile, true
	if path == "" {
		required = false
		if home, err := os.UserHomeDir(); err == nil {
			path = filepath.Join(home, ".my.cnf")
		}
	}
	if path != "" {
		vals, err := parseMyCnf(path, c.GroupSuffix)
		switch {
		case err == nil:
			for _, k := range []string{"host", "port", "socket", "user", "password"} {
				if vals[k] != "" {
					merged[k] = vals[k]
				}
			}
			if tls := tlsFromSSLMode(vals["ssl-mode"]); tls != "" {
				merged["tls"] = tls
			}
		case os.IsNotExist(err) && !required:
			// no ~/.my.cnf; fine
		default:
			return fmt.Errorf("reading MySQL options file: %w", err)
		}
	}

	if c.Credentials != "" {
		data, err := os.ReadFile(c.Credentials)
		if err != nil {
			return fmt.Errorf("reading -credentials file: %w", err)
		}
		var jf credFile
		if err := json.Unmarshal(data, &jf); err != nil {
			return fmt.Errorf("parsing %s: %w", c.Credentials, err)
		}
		for k, v := range map[string]string{
			"host": jf.Host, "socket": jf.Socket, "user": jf.User,
			"password": jf.Password, "tls": jf.TLS, "dsn": jf.DSN,
			"cloudsql-instance": jf.CloudSQLInstance,
		} {
			if v != "" {
				merged[k] = v
			}
		}
		if jf.Port != 0 {
			merged["port"] = strconv.Itoa(jf.Port)
		}
	}

	apply := func(flagName, envName string, dst *string) {
		if v := merged[flagName]; v != "" && !pinned(flagName, envName) {
			*dst = v
		}
	}
	apply("host", "PCS_HOST", &c.Host)
	apply("socket", "", &c.Socket)
	apply("user", "PCS_USER", &c.User)
	apply("password", "PCS_PASSWORD", &c.Password)
	apply("tls", "", &c.TLS)
	apply("dsn", "PCS_DSN", &c.DSN)
	apply("cloudsql-instance", "PCS_CLOUDSQL_INSTANCE", &c.CloudSQLInstance)
	if p, err := strconv.Atoi(merged["port"]); err == nil && !set["port"] {
		c.Port = p
	}
	return nil
}

// tlsFromSSLMode maps a my.cnf ssl-mode value onto the go-sql-driver
// tls parameter. REQUIRED encrypts without verifying the server cert,
// which is what skip-verify does; only VERIFY_CA/VERIFY_IDENTITY verify.
func tlsFromSSLMode(mode string) string {
	switch strings.ToUpper(mode) {
	case "PREFERRED":
		return "preferred"
	case "REQUIRED":
		return "skip-verify"
	case "VERIFY_CA", "VERIFY_IDENTITY":
		return "true"
	}
	return ""
}

// parseMyCnf reads the [client] section of a MySQL options file — plus
// [client<suffix>] when a group suffix is given, whose values win, the
// mysql --defaults-group-suffix convention. key = value lines, quotes
// stripped, #/; comments ignored; values containing '#' must be quoted,
// as with the mysql client.
func parseMyCnf(path, groupSuffix string) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	vals := map[string]string{}
	suffixVals := map[string]string{}
	target := vals // nil when inside an irrelevant section
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || line[0] == '#' || line[0] == ';' || strings.HasPrefix(line, "!include") {
			continue
		}
		if line[0] == '[' {
			switch section := strings.Trim(line, "[]"); {
			case strings.EqualFold(section, "client"):
				target = vals
			case groupSuffix != "" && strings.EqualFold(section, "client"+groupSuffix):
				target = suffixVals
			default:
				target = nil
			}
			continue
		}
		if target == nil {
			continue
		}
		key, val, found := strings.Cut(line, "=")
		if !found {
			continue // bare options like no-beep
		}
		key = strings.ToLower(strings.TrimSpace(key))
		val = strings.TrimSpace(val)
		if len(val) >= 2 && (val[0] == '"' || val[0] == '\'') && val[len(val)-1] == val[0] {
			val = val[1 : len(val)-1]
		} else if i := strings.Index(val, " #"); i >= 0 {
			val = strings.TrimSpace(val[:i]) // trailing comment
		}
		target[key] = val
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	maps.Copy(vals, suffixVals)
	return vals, nil
}
