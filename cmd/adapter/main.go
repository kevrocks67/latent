// @title Latent API
// @version 1.0
// @description Distributed artifact cache adapter for high-concurrency environments.
// @termsOfService http://swagger.io/terms/

// @contact.name API Support
// @contact.url http://github.com/kevindiaz/latent

// @host localhost:8080
// @BasePath /api/v1
package main

import (
	"log"

	"github.com/gin-gonic/gin"
	"github.com/kevrocks67/latent/internal/transport/http"
)

// @title Latent API
// @version 1.0
// @description Distributed artifact cache adapter.
// @host localhost:8060
// @BasePath /api/v1
func main() {
	r := gin.Default()

	// Initialize your handler (in a real app, you'd inject dependencies here)
	h := &http.Handler{}

	// Register all routes including Swagger
	h.RegisterRoutes(r)

	log.Println("Latent starting on :8060")
	log.Println("Swagger UI available at http://localhost:8080/swagger/index.html")

	if err := r.Run(":8060"); err != nil {
		log.Fatal("Failed to run server: ", err)
	}
}
