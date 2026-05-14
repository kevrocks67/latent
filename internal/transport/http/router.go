package http

import (
	"net/http"

	"github.com/gin-gonic/gin"

	// Swagger Middlewares
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"

	// IMPORTANT: Import the generated docs from YOUR project path
	// This blank import allows the swagger middleware to find your generated docs.
	_ "github.com/kevrocks67/latent/docs"
)

// Handler wraps the Cache Engine logic for the HTTP transport layer.
type Handler struct {
	// engine would be injected here
}

// RegisterRoutes sets up the API endpoints and the Swagger UI using Gin.
func (h *Handler) RegisterRoutes(r *gin.Engine) {
	// Grouping API routes for versioning
	v1 := r.Group("/api/v1")
	{
		// @Summary Fetch an artifact
		// @Router /fetch/{path} [get]
		v1.GET("/fetch/*path", h.HandleFetch)
	}

	// The Swagger UI Route
	// This serves the site at http://localhost:8080/swagger/index.html
	// 1. Explicit Redirect for the "naked" swagger path
	r.GET("/swagger", func(c *gin.Context) {
		c.Redirect(http.StatusMovedPermanently, "/swagger/index.html")
	})
	r.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))
}

// HandleFetch processes artifact requests.
func (h *Handler) HandleFetch(c *gin.Context) {
	path := c.Param("path")

	// Example logic:
	// In Gin, you use c.JSON, c.String, or c.Data to respond.
	// For Project Latent, we will eventually stream data here.
	c.JSON(http.StatusOK, gin.H{
		"message": "Latent fetch initiated",
		"path":    path,
	})
}
