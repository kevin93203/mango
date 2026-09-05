//go:build windows

package ipc

import "testing"

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
