package alfred

/*
#cgo CFLAGS: -w -I${SRCDIR}/alfred -std=gnu99 -D_GNU_SOURCE -fno-strict-aliasing
#cgo LDFLAGS: -pthread
#include <stdlib.h>
#include "binding.h"
*/
import "C"

import (
	"errors"
	"sync"
	"unsafe"
)

// ErrServerClosed is returned when operations are performed on a closed Server.
var ErrServerClosed = errors.New("alfred server is closed")

// ServerOption configures a Server instance created via NewServer.
type ServerOption func(*serverConfig)

type serverConfig struct {
	socketPath    string
	netInterface  string
	meshInterface string
	mode          Mode
	force         bool
}

// WithServerSocketPath overrides the UNIX socket path used by the server.
func WithServerSocketPath(path string) ServerOption {
	return func(cfg *serverConfig) {
		cfg.socketPath = path
	}
}

// WithServerInterface sets the primary network interface for announcements.
func WithServerInterface(iface string) ServerOption {
	return func(cfg *serverConfig) {
		cfg.netInterface = iface
	}
}

// WithServerMeshInterface sets the mesh (batman-adv) interface.
func WithServerMeshInterface(iface string) ServerOption {
	return func(cfg *serverConfig) {
		cfg.meshInterface = iface
	}
}

// WithServerMode selects primary or secondary server mode.
func WithServerMode(mode Mode) ServerOption {
	return func(cfg *serverConfig) {
		cfg.mode = mode
	}
}

// WithServerForce controls whether the server should take over an existing socket.
func WithServerForce(force bool) ServerOption {
	return func(cfg *serverConfig) {
		cfg.force = force
	}
}

// Server wraps the native alfred server implementation.
type Server struct {
	mu      sync.Mutex
	srv     *C.go_alfred_server
	running bool
	closed  bool
}

// NewServer constructs a Server using the provided options.
func NewServer(options ...ServerOption) (*Server, error) {
	cfg := serverConfig{
		socketPath:    DefaultSocketPath,
		netInterface:  "none",
		meshInterface: "none",
		mode:          ModeSecondary,
		force:         true,
	}

	for _, opt := range options {
		if opt != nil {
			opt(&cfg)
		}
	}

	var cPath *C.char
	if cfg.socketPath != "" {
		cPath = C.CString(cfg.socketPath)
		defer C.free(unsafe.Pointer(cPath))
	}

	var cNet *C.char
	if cfg.netInterface != "" {
		cNet = C.CString(cfg.netInterface)
		defer C.free(unsafe.Pointer(cNet))
	}

	var cMesh *C.char
	if cfg.meshInterface != "" {
		cMesh = C.CString(cfg.meshInterface)
		defer C.free(unsafe.Pointer(cMesh))
	}

	var errStr *C.char
	srv := C.go_alfred_server_new(cPath, cNet, cMesh, C.uint8_t(cfg.mode), boolToCInt(cfg.force), &errStr)
	if errStr != nil {
		defer C.go_alfred_client_free_string(errStr)
	}

	if srv == nil {
		return nil, errors.New(goString(errStr, "alfred: failed to create server"))
	}

	return &Server{srv: srv}, nil
}

// Start brings the server online if it is not already running.
func (s *Server) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrServerClosed
	}

	if s.running {
		return nil
	}

	var errStr *C.char
	status := C.go_alfred_server_start(s.srv, &errStr)
	if errStr != nil {
		defer C.go_alfred_client_free_string(errStr)
	}

	if status != 0 {
		return errors.New(goString(errStr, "alfred server start failed"))
	}

	s.running = true
	return nil
}

// Stop gracefully shuts down the running server.
func (s *Server) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrServerClosed
	}

	if !s.running {
		return nil
	}

	var errStr *C.char
	status := C.go_alfred_server_stop(s.srv, &errStr)
	if errStr != nil {
		defer C.go_alfred_client_free_string(errStr)
	}

	if status != 0 {
		return errors.New(goString(errStr, "alfred server stop failed"))
	}

	s.running = false
	return nil
}

// Close releases all resources associated with the server, stopping it if necessary.
func (s *Server) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	if s.running {
		var errStr *C.char
		status := C.go_alfred_server_stop(s.srv, &errStr)
		if errStr != nil {
			defer C.go_alfred_client_free_string(errStr)
		}
		if status == 0 {
			s.running = false
		}
	}

	C.go_alfred_server_free(s.srv)
	s.srv = nil
	s.closed = true

	return nil
}
