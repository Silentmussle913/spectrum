package server

import (
	"errors"
	"strings"

	"github.com/sandertv/gophertunnel/minecraft"
)

var ErrFallbackDisabled = errors.New("fallback is disabled")

// Discovery defines an interface for discovering servers based on a player's connection.
type Discovery interface {
	// Discover determines the primary server.
	Discover(conn *minecraft.Conn) (string, error)
	// DiscoverFallback determines the fallback server.
	DiscoverFallback(conn *minecraft.Conn) (string, error)
}

// StaticDiscovery implements the Discovery interface with static server addresses.
type StaticDiscovery struct {
	server         string
	fallbackServer string
}

// NewStaticDiscovery creates a new StaticDiscovery with the given server addresses.
func NewStaticDiscovery(server string, fallbackServer string) *StaticDiscovery {
	return &StaticDiscovery{
		server:         server,
		fallbackServer: fallbackServer,
	}
}

// Discover ...
func (s *StaticDiscovery) Discover(_ *minecraft.Conn) (string, error) {
	return s.server, nil
}

// DiscoverFallback ...
func (s *StaticDiscovery) DiscoverFallback(_ *minecraft.Conn) (string, error) {
	if strings.TrimSpace(s.fallbackServer) == "" {
		return "", ErrFallbackDisabled
	}
	return s.fallbackServer, nil
}
