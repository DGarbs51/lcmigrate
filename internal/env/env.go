package env

import "os"

// Provider provides environment variable access
type Provider interface {
	// Get returns the value of an environment variable
	Get(key string) string

	// GetWithFallback returns the first non-empty value from the given keys
	GetWithFallback(keys ...string) string
}

// OSProvider uses os.Getenv for real environment variables
type OSProvider struct{}

// NewOSProvider creates a new OS environment provider
func NewOSProvider() *OSProvider {
	return &OSProvider{}
}

// Get returns the value of an environment variable
func (p *OSProvider) Get(key string) string {
	return os.Getenv(key)
}

// GetWithFallback returns the first non-empty value from the given keys
func (p *OSProvider) GetWithFallback(keys ...string) string {
	for _, key := range keys {
		if val := os.Getenv(key); val != "" {
			return val
		}
	}
	return ""
}

// MapProvider uses an in-memory map (for testing)
type MapProvider struct {
	Vars map[string]string
}

// NewMapProvider creates a new map-based environment provider
func NewMapProvider(vars map[string]string) *MapProvider {
	if vars == nil {
		vars = make(map[string]string)
	}
	return &MapProvider{Vars: vars}
}

// Get returns the value from the map
func (p *MapProvider) Get(key string) string {
	return p.Vars[key]
}

// GetWithFallback returns the first non-empty value from the given keys
func (p *MapProvider) GetWithFallback(keys ...string) string {
	for _, key := range keys {
		if val := p.Vars[key]; val != "" {
			return val
		}
	}
	return ""
}

// Set sets a value in the map (useful for tests)
func (p *MapProvider) Set(key, value string) {
	p.Vars[key] = value
}
