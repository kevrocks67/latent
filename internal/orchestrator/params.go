package orchestrator

// authParams is a block-list of common query parameters used for authentication,
// signatures, or session tracking that should NOT be part of a deterministic cache key.
// Including these would result in cache misses whenever tokens expire or rotate.
var authParams = map[string]bool{
	// General & Legacy
	"token":        true,
	"access_token": true,
	"api_key":      true,
	"apikey":       true,
	"signature":    true,
	"sig":          true,
	"expires":      true,
	"state":        true,

	// AWS (S3 Signed URLs)
	"x-amz-algorithm":      true,
	"x-amz-credential":     true,
	"x-amz-date":           true,
	"x-amz-expires":        true,
	"x-amz-signedheaders":  true,
	"x-amz-signature":      true,
	"x-amz-security-token": true,

	// Google Cloud (GCS Signed URLs & Auth)
	"x-goog-algorithm":     true,
	"goog-hash":            true, // Often used for integrity, but can vary by transport
	"x-goog-credential":    true,
	"x-goog-date":          true,
	"x-goog-expires":       true,
	"x-goog-signedheaders": true,
	"x-goog-signature":     true,
	"authuser":             true, // Google multi-login parameter
	"key":                  true, // Common Google API Key param

	// Azure Blob Storage (SAS Tokens)
	"se":  true, // Signed Expiry
	"sp":  true, // Signed Permissions
	"st":  true, // Signed Start
	"sr":  true, // Signed Resource
	"srt": true, // Signed Resource Types
	"ss":  true, // Signed Services
	"sv":  true, // Signed Version

	// CDN Specifics
	"cf_token":    true, // Cloudflare
	"fastly_hash": true, // Fastly
}
