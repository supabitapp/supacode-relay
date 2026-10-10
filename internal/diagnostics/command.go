package diagnostics

import (
	"fmt"
	"io"

	"github.com/supabitapp/supacode-relay/internal/directory"
)

func Command(args []string, stdout, stderr io.Writer) (int, bool) {
	if len(args) < 2 || args[1] != "trace-tag" {
		return 0, false
	}
	if len(args) != 3 || !directory.ValidEndpointID(args[2]) {
		fmt.Fprintln(stderr, "usage: trace-tag <64-character endpoint ID>")
		return 2, true
	}
	if _, err := fmt.Fprintln(stdout, Tag(args[2])); err != nil {
		return 1, true
	}
	return 0, true
}
