package http

import (
	"bufio"
	"context"
	"io"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	// Swagger Middlewares
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"

	// IMPORTANT: Import the generated docs from YOUR project path
	_ "github.com/kevrocks67/latent/docs"
)

// Orchestrator defines the core contract for artifact lifecycle management.
// We use an interface here so the HTTP layer remains decoupled from the specific implementation.
type Orchestrator interface {
	// GetArtifact must accept context for cancellation and tracing propagation.
	Pull(ctx context.Context, path string) (io.ReadCloser, error)
}

// Handler wraps the Orchestrator to serve requests over HTTP.
type Handler struct {
	orchestrator Orchestrator
}

// NewHandler initializes the handler with the required Orchestrator.
func NewHandler(orc Orchestrator) *Handler {
	return &Handler{
		orchestrator: orc,
	}
}

// RegisterRoutes sets up the API endpoints and the Swagger UI using Gin.
func (h *Handler) RegisterRoutes(r *gin.Engine) {
	v1 := r.Group("/api/v1")
	{
		// @Summary Fetch an artifact
		// @Description Retrieves an artifact via the Orchestrator.
		// @Tags artifacts
		// @Param path path string true "Artifact Path"
		// @Router /fetch/{path} [get]
		v1.GET("/fetch/*path", h.HandleFetch)
	}

	r.GET("/swagger", func(c *gin.Context) {
		c.Redirect(http.StatusMovedPermanently, "/swagger/index.html")
	})
	r.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))
}

// HandleFetch processes artifact requests by delegating to the Orchestrator.
func (h *Handler) HandleFetch(c *gin.Context) {
	path := c.Param("path")
	targetUrl := strings.TrimPrefix(path, "/")

	// If orchestrator is not provided (test mode), return the path for validation.
	if h.orchestrator == nil {
		c.JSON(http.StatusOK, gin.H{"path": targetUrl})
		return
	}

	stream, err := h.orchestrator.Pull(c.Request.Context(), targetUrl)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Orchestrator failed to retrieve artifact",
			"path":  targetUrl,
			"msg":   err.Error(),
		})
		return
	}
	defer stream.Close()

	// 1. Wrap the stream in a buffered reader so we can "peek" at the start
	// Use a small peek so we don't block waiting for large buffers from slow upstreams.
	reader := bufio.NewReader(stream)
	// Default to a safe type
	contentType := "text/plain"

	// Sniff the content using a small, non-blocking peek (64 bytes).
	sniff, _ := reader.Peek(64)
	detected := http.DetectContentType(sniff)

	// Logic: If it looks like text/plain but contains HTML-ish markers, force text/html
	sniffStr := strings.ToLower(string(sniff))
	isLikelyHTML := strings.Contains(sniffStr, "<html") ||
		strings.Contains(sniffStr, "<!doctype") ||
		strings.Contains(sniffStr, "<script") ||
		strings.Contains(sniffStr, "<body") ||
		strings.Contains(sniffStr, "<head")

	if isLikelyHTML {
		contentType = "text/html; charset=utf-8"
	} else {
		contentType = detected
	}

	// 3. Stream the data to the client using DataFromReader.
	// We use -1 for contentLength if it is unknown, which will use chunked encoding.
	c.DataFromReader(http.StatusOK, -1, contentType, reader, nil)
}
