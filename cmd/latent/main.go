// @title Latent API
// @version 1.0
// @description Distributed artifact cache adapter for high-concurrency environments.
// @termsOfService http://swagger.io/terms/

// @contact.name API Support
// @contact.url http://github.com/kevindiaz/latent

// @host localhost:8060
// @BasePath /api/v1
package main

import "github.com/kevrocks67/latent/internal/cli"

// @title Latent API
// @version 1.0
// @description Distributed artifact cache adapter.
// @host localhost:8060
// @BasePath /api/v1
func main() {
	cli.Execute()
}
