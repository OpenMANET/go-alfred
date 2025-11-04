package main

import (
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	alfred "github.com/openmanet/go-alfred"
)

const (
	alfredVersion     = 0
	eventRegisterType = 9
	eventNotifyType   = 10
)

func detectDefaultInterface() string {
	ifaces, err := net.Interfaces()
	if err != nil {
		return "none"
	}

	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 {
			continue
		}
		if iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		if len(iface.HardwareAddr) != 6 {
			continue
		}
		return iface.Name
	}

	return "none"
}

func main() {
	var socketPath string
	var netInterface string
	var meshInterface string
	var modeFlag string
	var force bool

	defaultInterface := detectDefaultInterface()

	flag.StringVar(&socketPath, "socket", alfred.DefaultSocketPath, "alfred UNIX socket path")
	flag.StringVar(&netInterface, "iface", defaultInterface, "primary network interface for announcements")
	flag.StringVar(&meshInterface, "mesh", "none", "mesh interface (batman-adv) for announcements")
	flag.StringVar(&modeFlag, "mode", "secondary", "server mode: secondary or primary")
	flag.BoolVar(&force, "force", true, "force takeover of existing socket if present")
	flag.Parse()

	log.Printf("serving via socket %s", socketPath)
	log.Printf("primary interface: %s", netInterface)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serverMode := alfred.ModeSecondary
	switch modeFlag {
	case "secondary":
		serverMode = alfred.ModeSecondary
	case "primary":
		serverMode = alfred.ModePrimary
	default:
		log.Fatalf("invalid mode %q (use primary or secondary)", modeFlag)
	}

	server, err := alfred.NewServer(
		alfred.WithServerSocketPath(socketPath),
		alfred.WithServerInterface(netInterface),
		alfred.WithServerMeshInterface(meshInterface),
		alfred.WithServerMode(serverMode),
		alfred.WithServerForce(force),
	)
	if err != nil {
		log.Fatalf("failed to create server: %v", err)
	}
	defer func() {
		if err := server.Close(); err != nil {
			log.Printf("server close error: %v", err)
		}
	}()

	if err := server.Start(); err != nil {
		log.Fatalf("failed to start server: %v", err)
	}
	log.Printf("alfred server listening on %s", socketPath)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		monitorEvents(ctx, socketPath)
	}()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
	<-sigs

	log.Printf("shutting down")
	cancel()
	wg.Wait()

	if err := server.Stop(); err != nil && !errors.Is(err, alfred.ErrServerClosed) {
		log.Printf("server stop error: %v", err)
	}
}

func monitorEvents(ctx context.Context, socketPath string) {
	retryDelay := time.Second

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		conn, err := net.Dial("unix", socketPath)
		if err != nil {
			log.Printf("event monitor: dial failed: %v", err)
			if !sleepOrDone(ctx, retryDelay) {
				return
			}
			continue
		}

		if err := sendEventRegister(conn); err != nil {
			log.Printf("event monitor: register failed: %v", err)
			_ = conn.Close()
			if !sleepOrDone(ctx, retryDelay) {
				return
			}
			continue
		}

		log.Printf("event monitor: listening for data updates")
		if err := consumeEvents(ctx, conn); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("event monitor: %v", err)
		}

		_ = conn.Close()

		if !sleepOrDone(ctx, retryDelay) {
			return
		}
	}
}

func sendEventRegister(conn net.Conn) error {
	var buf [4]byte
	buf[0] = eventRegisterType
	buf[1] = alfredVersion
	binary.BigEndian.PutUint16(buf[2:], 0)
	_, err := conn.Write(buf[:])
	return err
}

func consumeEvents(ctx context.Context, conn net.Conn) error {
	header := make([]byte, 4)
	var payload []byte

	for {
		if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
			return err
		}

		if _, err := io.ReadFull(conn, header); err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				select {
				case <-ctx.Done():
					return context.Canceled
				default:
				}
				continue
			}
			return err
		}

		length := binary.BigEndian.Uint16(header[2:])
		if int(length) > cap(payload) {
			payload = make([]byte, length)
		} else {
			payload = payload[:length]
		}

		if length > 0 {
			if _, err := io.ReadFull(conn, payload); err != nil {
				if ne, ok := err.(net.Error); ok && ne.Timeout() {
					select {
					case <-ctx.Done():
						return context.Canceled
					default:
					}
					continue
				}
				return err
			}
		}

		if header[1] != alfredVersion {
			continue
		}

		switch header[0] {
		case eventNotifyType:
			if len(payload) < 7 {
				log.Printf("event monitor: short notify payload (%d bytes)", len(payload))
				continue
			}

			dataType := payload[0]
			source := net.HardwareAddr(payload[1:7])
			log.Printf("event: type=%d source=%s", dataType, source)
		default:
			// ignore other messages
		}
	}
}

func sleepOrDone(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
