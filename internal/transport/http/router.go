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

	"github.com/kevrocks67/latent/internal/logger"
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
		v1.GET("/fetch/*path", h.HandleFetch)
	}

	r.GET("/swagger", func(c *gin.Context) {
		c.Redirect(http.StatusMovedPermanently, "/swagger/index.html")
	})
	r.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))
}

// HandleFetch processes artifact requests by delegating to the Orchestrator.
// @Summary      Fetch an artifact
// @Description  Retrieves an upstream object via the orchestrator distribution layer or pulls from cache.
// @Tags         artifacts
// @Produce      json
// @Param        path  path      string  true  "Target Upstream Resource Path"
// @Success      200   {object}  map[string]interface{} "Returns status metadata or streams binary artifact payload"
// @Failure      500   {object}  map[string]string      "Returned when orchestrator pipeline fails to fetch resource"
// @Router /fetch/{path} [get]
func (h *Handler) HandleFetch(c *gin.Context) {
	path := c.Param("path")
	targetURL := strings.TrimPrefix(path, "/")

	// If orchestrator is not provided (test mode), return the path for validation.
	if h.orchestrator == nil {
		c.JSON(http.StatusOK, gin.H{"path": targetURL})
		return
	}

	stream, err := h.orchestrator.Pull(c.Request.Context(), targetURL)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Orchestrator failed to retrieve artifact",
			"path":  targetURL,
			"msg":   err.Error(),
		})
		return
	}
	defer func() {
		if err := stream.Close(); err != nil {
			logger.FromContext(c.Request.Context()).Warn("HTTP stream failed to close", "err", err)
		}
	}()

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
