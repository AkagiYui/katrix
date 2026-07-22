package csapi

import "github.com/AkagiYui/katrix/internal/roomver"

// roomVersionCapabilities returns the availability map for /capabilities.
func roomVersionCapabilities() map[string]string {
	return roomver.CapabilityMap()
}
