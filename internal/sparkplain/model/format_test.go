package model

import "testing"

func TestFormatters(t *testing.T) {
	cases := []struct{ got, want string }{
		{Bytes(512), "512 B"}, {Bytes(1536), "1.5 KiB"}, {Bytes(3 << 30), "3.0 GiB"}, {Bytes(300 << 20), "300 MiB"},
		{Duration(850), "850 ms"}, {Duration(4500), "4.5 s"}, {Duration(2832000), "47 min 12 s"}, {Duration(3780000), "1 h 3 min"},
		{Percent(0.42), "42%"}, {Percent(0.051), "5.1%"}, {Percent(0), "0%"},
		{Num(1234567), "1,234,567"}, {Num(-1000), "-1,000"}, {Num(12), "12"},
		{Plural(1, "job", "jobs"), "1 job"}, {Plural(3, "job", "jobs"), "3 jobs"},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("got %q, want %q", c.got, c.want)
		}
	}
	s := Source{File: "f", Line: 5}
	s.Extend(Source{File: "f", Line: 9})
	s.Extend(Source{File: "f", Line: 2})
	s.Extend(Source{File: "g", Line: 1})
	if s.String() != "f:2-9" {
		t.Errorf("source %s", s)
	}
}
