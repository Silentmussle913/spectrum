package session

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cooldogedev/spectrum/server"
	spectrumpacket "github.com/cooldogedev/spectrum/server/packet"
	"github.com/sandertv/gophertunnel/minecraft/protocol/packet"
)

// handleServer continuously reads packets from the server and forwards them to the client.
func handleServer(s *Session) {
	defer func() {
		if r := recover(); r != nil {
			s.CloseWithError(fmt.Errorf("panic while handling server packets: %v", r))
			logError(s, "panic while handling server packets", fmt.Errorf("%v", r))
		}
	}()
loop:
	for {
		select {
		case <-s.ctx.Done():
			s.CloseWithError(context.Cause(s.ctx))
			break loop
		default:
		}

		srv := s.Server()
		select {
		case <-srv.Context().Done():
			if s.Server() != srv {
				continue loop
			}
			serverErr := context.Cause(srv.Context())
			if serverErr == nil {
				serverErr = errors.New("backend server disconnected")
			}
			if err := s.fallback(); err != nil {
				if errors.Is(err, server.ErrFallbackDisabled) {
					s.CloseWithError(fmt.Errorf("backend server disconnected: %w", serverErr))
					logError(s, "backend server disconnected", serverErr)
					break loop
				}
				s.CloseWithError(fmt.Errorf("fallback failed: %w", err))
				logError(s, "failed to fallback to a different server", err)
				break loop
			}
			continue loop
		default:
		}

		batch, err := srv.ReadPacket()
		if err != nil {
			srv.CloseWithError(fmt.Errorf("failed to read packet from server: %w", err))
			if s.Server() != srv {
				continue loop
			}
			if fallbackErr := s.fallback(); fallbackErr != nil {
				if errors.Is(fallbackErr, server.ErrFallbackDisabled) {
					s.CloseWithError(fmt.Errorf("failed to read packet from server: %w", err))
					logError(s, "failed to read packet from server", err)
					break loop
				}
				s.CloseWithError(fmt.Errorf("fallback failed: %w", fallbackErr))
				logError(s, "failed to fallback to a different server", fallbackErr)
				break loop
			}
			continue loop
		}

		if pks, ok := batch.([][]byte); ok {
			for _, pk := range pks {
				ctx := NewContext()
				s.processor.ProcessServerEncoded(ctx, &pk)
				if ctx.Cancelled() {
					continue
				}

				if _, err := s.client.Write(pk); err != nil {
					s.CloseWithError(fmt.Errorf("failed to write packet to client: %w", err))
					logError(s, "failed to write packet to client", err)
					break loop
				}
			}
			continue loop
		}

		shouldFlush := true

		var packets []packet.Packet
		var isPackets bool

		packets, isPackets = batch.([]packet.Packet)
		if !isPackets {
			if pk, ok := batch.(packet.Packet); ok {
				packets = []packet.Packet{pk}
				shouldFlush = false
			} else {
				s.CloseWithError(fmt.Errorf("failed to read packet from server: %w", err))
				logError(s, "failed to read packet from server", err)
				break loop
			}
		}

		for _, pk := range packets {
			switch pk := pk.(type) {
			case *spectrumpacket.Flush:
				_ = s.client.Flush()
			case *spectrumpacket.Latency:
				s.latency.Store(pk.Latency)
			case *spectrumpacket.Transfer:
				if err := s.Transfer(TransferOptions{Address: pk.Addr, Args: pk.Args}); err != nil {
					logError(s, "failed to transfer", err)
				}
			default:
				ctx := NewContext()
				s.processor.ProcessServer(ctx, &pk)
				if ctx.Cancelled() {
					continue
				}

				if s.opts.SyncProtocol {
					for _, latest := range s.client.Proto().ConvertToLatest(pk, s.client) {
						s.tracker.handlePacket(latest)
					}
				} else {
					s.tracker.handlePacket(pk)
				}

				if err := s.WritePacketToClient(pk); err != nil {
					s.CloseWithError(fmt.Errorf("failed to write packet to client: %w", err))
					logError(s, "failed to write packet to client", err)
					break loop
				}
			}
		}

		if shouldFlush {
			if v, ok := s.Processor().(interface{ ProcessEndOfBatch(ctx *Context) }); ok {
				v.ProcessEndOfBatch(NewContext())
			} else {
				s.ClientFlush()
			}
		}
	}
}

// handleClient continuously reads packets from the client and forwards them to the server.
func handleClient(s *Session) {
	defer func() {
		if r := recover(); r != nil {
			s.CloseWithError(fmt.Errorf("panic while handling client packets: %v", r))
			logError(s, "panic while handling client packets", fmt.Errorf("%v", r))
		}
	}()

loop:
	for {
		select {
		case <-s.ctx.Done():
			s.CloseWithError(context.Cause(s.ctx))
			break loop
		default:
		}

		pk, err := s.client.ReadPacket()
		if err != nil {
			s.CloseWithError(fmt.Errorf("failed to read packet from client: %w", err))
			logError(s, "failed to read packets from client", err)
			break loop
		}

		if err := handleClientPackets(s, []packet.Packet{pk}); err != nil {
			s.Server().CloseWithError(fmt.Errorf("failed to write packet to server: %w", err))
		}
	}
}

// handleLatency periodically sends the client's current ping and timestamp to the server for latency reporting.
func handleLatency(s *Session, interval int64) {
	ticker := time.NewTicker(time.Millisecond * time.Duration(interval))
	defer ticker.Stop()
loop:
	for {
		select {
		case <-s.ctx.Done():
			s.CloseWithError(context.Cause(s.ctx))
			break loop
		case <-ticker.C:
			if err := s.WritePacketToServer(&spectrumpacket.Latency{
				Latency:          s.client.Latency().Milliseconds() * 2,
				Timestamp:        time.Now().UnixMilli(),
				ClientPacketLoss: 0,
			}); err != nil {
				logError(s, "failed to write latency packet", err)
			}
		}
	}
}

// handleClientPackets processes and forwards the provided packet from the client to the server.
func handleClientPackets(s *Session, packets []packet.Packet) (err error) {
	filtered := make([]packet.Packet, 0, len(packets))
	for _, pk := range packets {
		ctx := NewContext()
		s.Processor().ProcessClient(ctx, &pk)
		if ctx.Cancelled() {
			continue
		}
		filtered = append(filtered, pk)
	}
	if err := s.WritePacketsToServer(filtered); err != nil {
		return err
	}
	return nil
}

func logError(s *Session, msg string, err error) {
	select {
	case <-s.ctx.Done():
		return
	default:
	}

	if !errors.Is(err, context.Canceled) {
		s.logger.Error(msg, "err", err)
	}
}
