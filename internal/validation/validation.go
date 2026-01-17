// Package validation provides input validation functions for the duckarrow extension.
// These are separated from the main package to enable unit testing without CGO.
package validation

import (
	"fmt"
	"net/url"
	"strings"
)

// ValidateTableName checks if a table name is safe to use in SQL queries.
// This prevents SQL injection attacks by rejecting names containing dangerous characters.
// The query is sent to a remote Flight SQL server, so we must validate thoroughly.
func ValidateTableName(name string) error {
	if len(name) == 0 {
		return fmt.Errorf("table name cannot be empty")
	}
	if len(name) > 255 {
		return fmt.Errorf("table name exceeds maximum length of 255 characters")
	}
	// Reject characters that could enable SQL injection on the remote server
	// Even though we quote identifiers, these patterns are dangerous
	dangerousPatterns := []string{
		";",    // Statement terminator
		"--",   // SQL line comment
		"/*",   // SQL block comment start
		"*/",   // SQL block comment end
		"\x00", // Null byte
		"\n",   // Newline
		"\r",   // Carriage return
		"\t",   // Tab (can cause parsing confusion)
	}
	for _, pattern := range dangerousPatterns {
		if strings.Contains(name, pattern) {
			return fmt.Errorf("table name contains invalid characters")
		}
	}
	return nil
}

// ValidateURI performs validation on the gRPC URI.
// It checks for:
// - Non-empty URI
// - Valid grpc:// or grpc+tls:// scheme
// - Presence of host component
// - Reasonable length limit
func ValidateURI(uri string) error {
	uri = strings.TrimSpace(uri)
	if uri == "" {
		return fmt.Errorf("URI cannot be empty")
	}

	// Check length limit to prevent abuse
	if len(uri) > 2048 {
		return fmt.Errorf("URI exceeds maximum length of 2048 characters")
	}

	// Check for valid scheme prefix
	var hostPart string
	if strings.HasPrefix(uri, "grpc+tls://") {
		hostPart = strings.TrimPrefix(uri, "grpc+tls://")
	} else if strings.HasPrefix(uri, "grpc://") {
		hostPart = strings.TrimPrefix(uri, "grpc://")
	} else {
		return fmt.Errorf("URI must start with grpc:// or grpc+tls://")
	}

	// Check that host is present
	if hostPart == "" {
		return fmt.Errorf("URI must include a host")
	}

	// Use net/url to validate the host:port format
	// We prepend "http://" temporarily since url.Parse requires a known scheme
	testURL, err := url.Parse("http://" + hostPart)
	if err != nil {
		return fmt.Errorf("invalid URI format: %v", err)
	}

	if testURL.Host == "" {
		return fmt.Errorf("URI must include a valid host")
	}

	return nil
}
