package web

import (
	"context"
	"embed"
	"fmt"
	"html/template"
	"io/fs"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/thieso2/cio/config"
	"github.com/thieso2/cio/resolver"
	"github.com/thieso2/cio/resource"
)

//go:embed templates/* templates/partials/*
var templates embed.FS

//go:embed static/*
var static embed.FS

// Server represents the web UI server
type Server struct {
	cfg      *config.Config
	resolver *resolver.Resolver
	factory  *resource.Factory
	router   *chi.Mux
	server   *http.Server
	tmpl     *template.Template
}

// New creates a new web server instance
func New(cfg *config.Config) (*Server, error) {
	r := resolver.Create(cfg)
	factory := resource.CreateFactory(r.ReverseResolve)

	// Parse templates
	tmpl, err := template.ParseFS(templates, "templates/*.html", "templates/partials/*.html")
	if err != nil {
		return nil, fmt.Errorf("failed to parse templates: %w", err)
	}

	s := &Server{
		cfg:      cfg,
		resolver: r,
		factory:  factory,
		tmpl:     tmpl,
	}

	s.setupRoutes()

	return s, nil
}

// setupRoutes configures the Chi router with all endpoints
func (s *Server) setupRoutes() {
	r := chi.NewRouter()

	// Middleware
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(60 * time.Second))
	r.Use(corsMiddleware) // Localhost only

	// Static files - serve from embedded FS
	staticFS, err := fs.Sub(static, "static")
	if err != nil {
		panic(fmt.Sprintf("failed to create static sub-filesystem: %v", err))
	}
	r.Handle("/static/*", http.StripPrefix("/static/", http.FileServer(http.FS(staticFS))))

	// API endpoints
	r.Route("/api", func(r chi.Router) {
		r.Get("/health", s.handleHealth)
		r.Get("/aliases", s.handleAliases)
		r.Get("/browse", s.handleBrowse)
		r.Get("/info", s.handleInfo)
		r.Get("/preview", s.handlePreview)
		r.Get("/search", s.handleSearch)
	})

	// Main UI
	r.Get("/", s.handleIndex)

	s.router = r
}

// corsMiddleware restricts access to localhost only
func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "http://localhost:*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// Start starts the HTTP server
func (s *Server) Start() error {
	addr := fmt.Sprintf("%s:%d", s.cfg.Server.Host, s.cfg.Server.Port)

	s.server = &http.Server{
		Addr:         addr,
		Handler:      s.router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	fmt.Printf("Starting web server at http://%s\n", addr)
	return s.server.ListenAndServe()
}

// Shutdown gracefully shuts down the server
func (s *Server) Shutdown(ctx context.Context) error {
	if s.server != nil {
		return s.server.Shutdown(ctx)
	}
	return nil
}
