//go:build !unix

package dashboard

import "time"

func processCPU() (time.Duration, bool) {
	return 0, false
}
