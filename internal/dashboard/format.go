package dashboard

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

func formatCount(n int64) string {
	digits := strconv.FormatInt(n, 10)
	var b strings.Builder
	for i, digit := range digits {
		if i > 0 && (len(digits)-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(digit)
	}
	return b.String()
}

func formatBytes(n float64) string {
	if n < 1024 {
		return fmt.Sprintf("%.0f B", n)
	}
	const units = "KMGTPE"
	unit := -1
	for n >= 1024 && unit < len(units)-1 {
		n /= 1024
		unit++
	}
	return fmt.Sprintf("%.1f %ciB", n, units[unit])
}

func formatQuantity(n float64) string {
	if n < 10 {
		return strconv.FormatFloat(n, 'f', 1, 64)
	}
	return formatCount(int64(math.Round(n)))
}

func formatLoad(load float64) string {
	return strconv.FormatFloat(load, 'f', 2, 64)
}

func formatPercent(fraction float64) string {
	return strconv.FormatFloat(fraction*100, 'f', 1, 64) + "%"
}

func formatUptime(d time.Duration) string {
	days := d / (24 * time.Hour)
	hours := d % (24 * time.Hour) / time.Hour
	minutes := d % time.Hour / time.Minute
	seconds := d % time.Minute / time.Second
	switch {
	case days > 0:
		return fmt.Sprintf("%dd %dh %dm", days, hours, minutes)
	case hours > 0:
		return fmt.Sprintf("%dh %dm", hours, minutes)
	case minutes > 0:
		return fmt.Sprintf("%dm %ds", minutes, seconds)
	default:
		return fmt.Sprintf("%ds", seconds)
	}
}
