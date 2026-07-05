package db

import (
	"fmt"
	"strings"
)

// Version is a parsed MySQL server version. Raw keeps the full server
// string (e.g. "8.0.36-28" on Percona, "5.7.44-log").
type Version struct {
	Major, Minor, Patch int
	Raw                 string
}

// ParseVersion extracts the leading numeric x.y.z from a server version
// string, tolerating suffixes like "-log", "-28", "-google".
func ParseVersion(raw string) Version {
	v := Version{Raw: raw}
	parts := strings.SplitN(raw, ".", 3)
	nums := []*int{&v.Major, &v.Minor, &v.Patch}
	for i, p := range parts {
		if i >= len(nums) {
			break
		}
		n := 0
		for _, r := range p {
			if r < '0' || r > '9' {
				break
			}
			n = n*10 + int(r-'0')
		}
		*nums[i] = n
	}
	return v
}

// AtLeast reports whether the server version is >= major.minor.patch.
func (v Version) AtLeast(major, minor, patch int) bool {
	if v.Major != major {
		return v.Major > major
	}
	if v.Minor != minor {
		return v.Minor > minor
	}
	return v.Patch >= patch
}

func (v Version) String() string {
	return fmt.Sprintf("%d.%d.%d", v.Major, v.Minor, v.Patch)
}
