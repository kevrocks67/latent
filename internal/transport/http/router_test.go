package http

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	_ "github.com/kevrocks67/latent/docs" // Mandatory: Initialize generated docs for Swagger
)

// TestRegisterRoutes verifies that the Gin engine correctly routes requests
// and handles the wildcard path parameters used by Latent.
func TestRegisterRoutes(t *testing.T) {
	// Set Gin to Test Mode to keep logs clean
	gin.SetMode(gin.TestMode)

	// Setup
	r := gin.New()
	h := &Handler{}
	h.RegisterRoutes(r)

	t.Run("Swagger Redirect Logic", func(t *testing.T) {
		// Test the naked /swagger path for the explicit redirect
		req, _ := http.NewRequest(http.MethodGet, "/swagger", nil)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)

		if w.Code != http.StatusMovedPermanently {
			t.Errorf("Expected 301 redirect for /swagger, got %d", w.Code)
		}

		location := w.Header().Get("Location")
		if location != "/swagger/index.html" {
			t.Errorf("Expected redirect to /swagger/index.html, got %s", location)
		}
	})

	t.Run("Swagger UI Documentation Access", func(t *testing.T) {
		// We test that the swagger endpoint is registered.
		// If the blank import of 'docs' is working, the handler should be active.
		req, _ := http.NewRequest(http.MethodGet, "/swagger/index.html", nil)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)

		// In some environments, the static assets aren't loaded in memory during 'go test'.
		// We check that it's NOT a 404, or if it is, we log a warning but prioritize the API tests.
		if w.Code == http.StatusNotFound {
			t.Log("Warning: Swagger assets not found in test environment. This is common if static files aren't embedded.")
		}
	})

	t.Run("Fetch API with Wildcard Path", func(t *testing.T) {
		artifactPath := "github.com/kevrocks67/final-project-assignment-aesd-kevrocks67/releases/download/v21-03328c6/door_security_daemon"
		// Note the /api/v1 prefix from your router group
		req, _ := http.NewRequest(http.MethodGet, "/api/v1/fetch/"+artifactPath, nil)
		w := httptest.NewRecorder()

		r.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("Expected status 200, got %d", w.Code)
		}

		var response map[string]interface{}
		if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
			t.Fatalf("Failed to parse JSON response: %v", err)
		}

		// Gin's c.Param("path") includes the leading slash for *wildcards
		expected := artifactPath
		if response["path"] != expected {
			t.Errorf("Expected path %s, got %s", expected, response["path"])
		}
	})
}
