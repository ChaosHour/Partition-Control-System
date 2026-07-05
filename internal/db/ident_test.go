package db

import "testing"

func TestQuoteIdent(t *testing.T) {
	tests := []struct{ in, want string }{
		{"orders", "`orders`"},
		{"weird name", "`weird name`"},
		{"back`tick", "`back``tick`"},
		{"drop`; --", "`drop``; --`"},
	}
	for _, tt := range tests {
		if got := QuoteIdent(tt.in); got != tt.want {
			t.Errorf("QuoteIdent(%q) = %s, want %s", tt.in, got, tt.want)
		}
	}
}

func TestQuoteQualified(t *testing.T) {
	if got := QuoteQualified("mydb", "orders"); got != "`mydb`.`orders`" {
		t.Errorf("QuoteQualified = %s", got)
	}
}
