package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type Publisher struct {
	c *redis.Client
}

func NewPublisher(c *redis.Client) *Publisher { return &Publisher{c: c} }

func OpenRedis(ctx context.Context, url string) (*redis.Client, error) {
	opt, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("parse redis url: %w", err)
	}
	c := redis.NewClient(opt)

	// Retry ping: redis DNS/socket may not be ready at boot.
	const maxAttempts = 30
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		lastErr = c.Ping(pingCtx).Err()
		cancel()
		if lastErr == nil {
			return c, nil
		}
		if ctx.Err() != nil {
			break
		}
		time.Sleep(2 * time.Second)
	}
	_ = c.Close()
	return nil, fmt.Errorf("ping redis after %d attempts: %w", maxAttempts, lastErr)
}

func (p *Publisher) Publish(ctx context.Context, channel string, payload any) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return p.c.Publish(ctx, channel, b).Err()
}
