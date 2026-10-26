package analyze

import "testing"

// A worker's log that says 127.0.0.1 matches the event log's localhost.
func TestHostKeyLoopback(t *testing.T) {
	t.Parallel()
	for in, want := range map[string]string{
		"127.0.0.1": "localhost", "localhost": "localhost", "::1": "localhost",
		"10.0.2.10": "ip-10-0-2-10", "ip-10-0-2-10.ec2.internal": "ip-10-0-2-10",
	} {
		if got := hostKey(in); got != want {
			t.Errorf("hostKey(%q) = %q, want %q", in, got, want)
		}
	}
}
