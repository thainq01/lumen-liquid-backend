// Package wsgateway provides a WebSocket hub that bridges Redis pub/sub to
// connected clients. The TradeCache is the single Redis consumer (it subscribes
// to events:global); when it changes a trader's state it notifies the hub,
// which pushes a fresh snapshot to that trader's subscribers.
package wsgateway

import (
	"context"
	"sync"

	"github.com/rs/zerolog"
)

// Hub manages WebSocket client connections and per-trader subscriptions.
// It does not talk to Redis directly — the TradeCache does, and notifies the
// hub via a callback when a trader's open trades change.
type Hub struct {
	clients    map[*Client]bool
	register   chan *Client
	unregister chan *Client
	broadcast  chan *Message
	logger     zerolog.Logger
	cache      *TradeCache
	mu         sync.RWMutex

	// subscriptions tracks which clients are subscribed to which channels
	subscriptions map[string]map[*Client]bool
}

// Message is a broadcast message to be sent to subscribed clients.
type Message struct {
	Channel string
	Payload []byte
}

// NewHub creates a WebSocket hub wired to the cache. Call Run() to start.
func NewHub(cache *TradeCache, logger zerolog.Logger) *Hub {
	h := &Hub{
		clients:       make(map[*Client]bool),
		register:      make(chan *Client),
		unregister:    make(chan *Client),
		broadcast:     make(chan *Message, 256),
		logger:        logger,
		cache:         cache,
		subscriptions: make(map[string]map[*Client]bool),
	}
	// On any real cache change for a trader, push a fresh full snapshot to
	// that trader's subscribers AND to the global "trades:all" subscribers.
	cache.SetOnChange(func(trader string) {
		if snapshot, err := cache.GetSnapshot(trader); err == nil {
			h.broadcast <- &Message{Channel: "trader:" + trader, Payload: snapshot}
		}
		if allSnap, err := cache.GetAllSnapshot(); err == nil {
			h.broadcast <- &Message{Channel: "trades:all", Payload: allSnap}
		}
	})
	return h
}

// Register registers a client with the hub.
func (h *Hub) Register(client *Client) {
	h.register <- client
}

// Run processes client registrations, unregistrations, and broadcasts.
// Blocks until ctx is canceled.
func (h *Hub) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			h.logger.Info().Msg("hub shutting down")
			h.closeAll()
			return

		case client := <-h.register:
			h.mu.Lock()
			h.clients[client] = true
			h.mu.Unlock()
			h.logger.Debug().Str("remote", client.RemoteAddr).Msg("client registered")

		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.send)
				for _, subs := range h.subscriptions {
					delete(subs, client)
				}
			}
			h.mu.Unlock()
			h.logger.Debug().Str("remote", client.RemoteAddr).Msg("client unregistered")

		case msg := <-h.broadcast:
			h.mu.RLock()
			subs := h.subscriptions[msg.Channel]
			for client := range subs {
				select {
				case client.send <- msg.Payload:
				default:
					close(client.send)
					delete(h.clients, client)
				}
			}
			h.mu.RUnlock()
		}
	}
}

// Subscribe subscribes a client to a channel and immediately sends the current
// snapshot for trader channels.
func (h *Hub) Subscribe(client *Client, channel string) {
	h.mu.Lock()
	if h.subscriptions[channel] == nil {
		h.subscriptions[channel] = make(map[*Client]bool)
	}
	h.subscriptions[channel][client] = true
	h.mu.Unlock()

	h.logger.Debug().Str("remote", client.RemoteAddr).Str("channel", channel).Msg("client subscribed")

	// Send current state snapshot for trader channels
	if h.cache != nil && len(channel) > 7 && channel[:7] == "trader:" {
		trader := channel[7:]
		if snapshot, err := h.cache.GetSnapshot(trader); err == nil {
			select {
			case client.send <- snapshot:
			default:
			}
		}
	}
	// Send all-trades snapshot for the global channel
	if h.cache != nil && channel == "trades:all" {
		if snapshot, err := h.cache.GetAllSnapshot(); err == nil {
			select {
			case client.send <- snapshot:
			default:
			}
		}
	}
}

// Unsubscribe removes a client from a channel.
func (h *Hub) Unsubscribe(client *Client, channel string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if subs, ok := h.subscriptions[channel]; ok {
		delete(subs, client)
	}
	h.logger.Debug().Str("remote", client.RemoteAddr).Str("channel", channel).Msg("client unsubscribed")
}

// closeAll closes all client connections.
func (h *Hub) closeAll() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for client := range h.clients {
		close(client.send)
	}
	h.clients = make(map[*Client]bool)
	h.subscriptions = make(map[string]map[*Client]bool)
}
