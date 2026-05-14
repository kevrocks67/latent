package valkey

import (
	"context"
	"fmt"
	"time"

	valkey "github.com/valkey-io/valkey-go"
)

type ValkeyCoordinator struct {
	client valkey.Client
}

func NewCoordinator(client valkey.Client) *ValkeyCoordinator {
	return &ValkeyCoordinator{client: client}
}

func (vc *ValkeyCoordinator) AcquireLock(ctx context.Context, key string, nodeId string, ttl time.Duration) (bool, error) {
	cmd := vc.client.B().Set().Key(key).Value(nodeId).Nx().Px(ttl).Build()

	err := vc.client.Do(ctx, cmd).Error()
	if err == nil {
		return true, nil
	}

	// If lock already exists
	if valkey.IsValkeyNil(err) {
		return false, nil
	}

	return false, fmt.Errorf("ValkeyCoordinator failed to acquire lock: %w", err)
}

func (vc *ValkeyCoordinator) ReleaseLock(ctx context.Context, key string, nodeId string) error {
	// Only delete lock if nodeId matches value in key
	script := "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end"
	cmd := vc.client.B().Eval().Script(script).Numkeys(1).Key(key).Arg(nodeId).Build()
	err := vc.client.Do(ctx, cmd).Error()

	if err != nil {
		return fmt.Errorf("ValkeyCoordinator failed to release lock: %w", err)
	}
	return nil
}

func (vc *ValkeyCoordinator) WaitForReady(ctx context.Context, key string) error {
	sKey := fmt.Sprintf("%s:status", key)

	// Buffered channel to ensure we don't block the Valkey pipeline goroutine
	readySignal := make(chan struct{}, 1)

	// Create a context that we can cancel once we get our signal
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// We only check if the key exists AFTER the subscription is active.
	hookCtx := valkey.WithOnSubscriptionHook(subCtx, func(s valkey.PubSubSubscription) {
		if s.Kind == "subscribe" && s.Channel == sKey {
			// Subscription is confirmed. Now check if the work was already finished.
			go func() {
				exists, err := vc.client.Do(ctx, vc.client.B().Exists().Key(sKey).Build()).AsInt64()
				if err == nil && exists > 0 {
					select {
					case readySignal <- struct{}{}:
					default:
					}
				}
			}()
		}
	})

	// Run Receive in a goroutine
	go func() {
		_ = vc.client.Receive(hookCtx, vc.client.B().Subscribe().Channel(sKey).Build(), func(msg valkey.PubSubMessage) {
			select {
			case readySignal <- struct{}{}:
			default:
			}
		})
	}()

	select {
	case <-readySignal:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (vc *ValkeyCoordinator) SignalReady(ctx context.Context, key string) error {
	sKey := fmt.Sprintf("%s:status", key)

	// Atomic Set + Publish.
	script := `
		redis.call("SET", KEYS[1], "1", "EX", ARGV[1])
		return redis.call("PUBLISH", KEYS[1], "1")
	`
	return vc.client.Do(ctx, vc.client.B().Eval().Script(script).Numkeys(1).Key(sKey).Arg("3600").Build()).Error()
}

func (vc *ValkeyCoordinator) Close() {
	vc.client.Close()
}
