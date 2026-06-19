package wsgateway

import (
	"context"
	"encoding/json"
	"time"

	"github.com/coder/websocket"
	"github.com/rs/zerolog"
)

const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = (pongWait * 9) / 10
	maxMessageSize = 512
)

// Client represents a single WebSocket connection.
type Client struct {
	hub        *Hub
	conn       *websocket.Conn
	send       chan []byte
	RemoteAddr string
	logger     zerolog.Logger
}

// ClientMessage is a message from the client (subscription commands).
type ClientMessage struct {
	Type    string `json:"type"`    // "subscribe" | "unsubscribe"
	Channel string `json:"channel"` // "trader:ADDR" | "pair:N" | "prices"
}

// ServerMessage is a message sent to the client.
type ServerMessage struct {
	Channel string          `json:"channel"`
	Event   json.RawMessage `json:"event"`
}

// NewClient creates a new WebSocket client.
func NewClient(hub *Hub, conn *websocket.Conn, remoteAddr string, logger zerolog.Logger) *Client {
	return &Client{
		hub:        hub,
		conn:       conn,
		send:       make(chan []byte, 256),
		RemoteAddr: remoteAddr,
		logger:     logger,
	}
}

// ReadPump reads messages from the WebSocket connection and processes subscription commands.
func (c *Client) ReadPump(ctx context.Context) {
	defer func() {
		c.hub.unregister <- c
		c.conn.Close(websocket.StatusNormalClosure, "")
	}()

	c.conn.SetReadLimit(maxMessageSize)

	for {
		_, data, err := c.conn.Read(ctx)
		if err != nil {
			if websocket.CloseStatus(err) == websocket.StatusNormalClosure {
				c.logger.Debug().Msg("client closed connection")
			} else {
				c.logger.Warn().Err(err).Msg("read error")
			}
			break
		}

		var msg ClientMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			c.logger.Warn().Err(err).Str("data", string(data)).Msg("invalid message")
			continue
		}

		switch msg.Type {
		case "subscribe":
			if msg.Channel == "" {
				c.logger.Warn().Msg("subscribe missing channel")
				continue
			}
			c.hub.Subscribe(c, msg.Channel)

		case "unsubscribe":
			if msg.Channel == "" {
				c.logger.Warn().Msg("unsubscribe missing channel")
				continue
			}
			c.hub.Unsubscribe(c, msg.Channel)

		default:
			c.logger.Warn().Str("type", msg.Type).Msg("unknown message type")
		}
	}
}

// WritePump writes messages from the send channel to the WebSocket connection.
func (c *Client) WritePump(ctx context.Context) {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close(websocket.StatusNormalClosure, "")
	}()

	for {
		select {
		case <-ctx.Done():
			return

		case message, ok := <-c.send:
			if !ok {
				// Hub closed the channel
				c.conn.Close(websocket.StatusNormalClosure, "hub closed")
				return
			}

			writeCtx, cancel := context.WithTimeout(ctx, writeWait)
			err := c.conn.Write(writeCtx, websocket.MessageText, message)
			cancel()
			if err != nil {
				c.logger.Warn().Err(err).Msg("write error")
				return
			}

		case <-ticker.C:
			// Send ping
			writeCtx, cancel := context.WithTimeout(ctx, writeWait)
			err := c.conn.Ping(writeCtx)
			cancel()
			if err != nil {
				c.logger.Warn().Err(err).Msg("ping error")
				return
			}
		}
	}
}
