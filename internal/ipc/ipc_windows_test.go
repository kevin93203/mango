//go:build windows

package ipc

import "testing"

func TestPipeSecurityDescriptorAuthorizesCurrentUser(t *testing.T) {
	const userSID = "S-1-5-21-111111111-222222222-333333333-1001"
	want := "D:P(A;;GA;;;OW)(A;;GA;;;" + userSID + ")"
	if got := pipeSecurityDescriptor(userSID); got != want {
		t.Fatalf("pipe security descriptor = %q, want %q", got, want)
	}
}

func TestPipeNameUsesNormalizedInstanceRoot(t *testing.T) {
	first := pipeNameForEndpoint(`C:\Users\Test\Mango\runtime\mango.sock`)
	second := pipeNameForEndpoint(`c:\users\test\mango\runtime\other.sock`)
	third := pipeNameForEndpoint(`C:\Users\Other\Mango\runtime\mango.sock`)
	if first != second {
		t.Fatalf("same instance roots produced different pipe names: %q != %q", first, second)
	}
	if first == third {
		t.Fatalf("different instance roots produced the same pipe name: %q", first)
	}
	if first == `C:\Users\Test\Mango\runtime\mango.sock` {
		t.Fatal("pipe name exposed the endpoint path")
	}
}
