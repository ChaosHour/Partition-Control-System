package pcs

import "testing"

func TestHumanBytes(t *testing.T) {
	cases := []struct {
		in   int64
		want string
	}{
		{0, "0 B"},
		{999, "999 B"},
		{1024, "1.0 KiB"},
		{1536, "1.5 KiB"},
		{4 << 30, "4.0 GiB"},
		{20 << 30, "20.0 GiB"},
	}
	for _, c := range cases {
		if got := HumanBytes(c.in); got != c.want {
			t.Errorf("HumanBytes(%d) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestComma(t *testing.T) {
	cases := []struct {
		in   int64
		want string
	}{
		{0, "0"},
		{999, "999"},
		{1000, "1,000"},
		{1234567, "1,234,567"},
		{3341579421, "3,341,579,421"},
		{-1234, "-1,234"},
	}
	for _, c := range cases {
		if got := Comma(c.in); got != c.want {
			t.Errorf("Comma(%d) = %q, want %q", c.in, got, c.want)
		}
	}
}
