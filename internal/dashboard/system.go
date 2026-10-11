package dashboard

import (
	"os"
	"strconv"
	"strings"
)

func loadAverage() (load [3]float64, ok bool) {
	data, err := os.ReadFile("/proc/loadavg")
	if err != nil {
		return load, false
	}
	return parseLoadAverage(string(data))
}

func parseLoadAverage(loadavg string) (load [3]float64, ok bool) {
	fields := strings.Fields(loadavg)
	if len(fields) < len(load) {
		return load, false
	}
	for i := range load {
		value, err := strconv.ParseFloat(fields[i], 64)
		if err != nil {
			return load, false
		}
		load[i] = value
	}
	return load, true
}

func systemMemory() (used, total uint64, ok bool) {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0, 0, false
	}
	return parseMeminfo(string(data))
}

func parseMeminfo(meminfo string) (used, total uint64, ok bool) {
	var available uint64
	var haveTotal, haveAvailable bool
	for line := range strings.Lines(meminfo) {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		kib, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			continue
		}
		switch fields[0] {
		case "MemTotal:":
			total, haveTotal = kib<<10, true
		case "MemAvailable:":
			available, haveAvailable = kib<<10, true
		}
	}
	if !haveTotal || !haveAvailable || total == 0 || available > total {
		return 0, 0, false
	}
	return total - available, total, true
}
