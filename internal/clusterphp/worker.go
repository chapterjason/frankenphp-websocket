package clusterphp

import (
	"context"
	"fmt"

	"github.com/caddyserver/caddy/v2"
	"github.com/chapterjason/frankenphp-websocket/internal/cluster"
	"github.com/dunglas/frankenphp"
	frankenphpcaddy "github.com/dunglas/frankenphp/caddy"
	"go.uber.org/zap"
)

func init() {
	cluster.SetWorkerSetup(setupWorker)
}

// setupWorker registers the worker of `cluster member` with FrankenPHP and
// makes it take the messages of the PHP channel.
func setupWorker(ctx caddy.Context, membership *cluster.Membership, worker cluster.Worker) error {
	// Loading the app makes FrankenPHP start, and with it the registered workers.
	if _, err := ctx.App("frankenphp"); err != nil {
		return fmt.Errorf("cluster member: loading frankenphp app: %w", err)
	}

	workers := frankenphpcaddy.RegisterWorkers("cluster:"+worker.File, worker.File, worker.Num)
	membership.Handle(cluster.MessagesChannel, handler(workers, ctx.Logger()))
	membership.OnMemberLeft(leftHandler(workers, ctx.Logger()))

	return nil
}

// leftHandler passes a member that left to the worker as a left event,
// outside the membership's goroutines.
func leftHandler(workers frankenphp.Workers, logger *zap.Logger) func(cluster.MemberLeft) {
	return func(left cluster.MemberLeft) {
		event := frankenphp.AssociativeArray[any]{
			Map: map[string]any{
				"event":    "left",
				"instance": left.Instance,
				"reason":   left.Reason,
				"role":     left.Role.String(),
			},
			Order: []string{"event", "instance", "reason", "role"},
		}

		go func() {
			if _, err := workers.SendMessage(context.Background(), event, nil); err != nil {
				logger.Error("cluster left event failed", zap.String("instance", left.Instance), zap.Error(err))
			}
		}()
	}
}

// handler passes every message to the worker as an event, outside the bus's
// reader, and answers a request with the worker's return value: a string, an
// empty one for null.
func handler(workers frankenphp.Workers, logger *zap.Logger) cluster.Handler {
	return func(message cluster.Message, respond func([]byte)) {
		event := messageEvent(message)

		go func() {
			reply, err := workers.SendMessage(context.Background(), event, nil)
			if err != nil {
				logger.Error("cluster message failed", zap.String("from", message.From), zap.Error(err))

				return
			}
			if !message.Request {
				return
			}

			switch answer := reply.(type) {
			case nil:
				respond(nil)
			case string:
				respond([]byte(answer))
			default:
				logger.Error("cluster worker answer ignored: expected string or null", zap.String("go_type", fmt.Sprintf("%T", answer)))
			}
		}()
	}
}

// messageEvent is what PHP gets for a message of another member, from the
// worker and from cluster_receive() alike.
func messageEvent(message cluster.Message) frankenphp.AssociativeArray[any] {
	return frankenphp.AssociativeArray[any]{
		Map: map[string]any{
			"event":   "message",
			"from":    message.From,
			"data":    string(message.Data),
			"request": message.Request,
		},
		Order: []string{"event", "from", "data", "request"},
	}
}
