package clusterphp

// #include <stdint.h>
// #include "cli.h"
import "C"
import (
	"os"
	"runtime/cgo"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/caddyserver/caddy/v2"
	"github.com/chapterjason/frankenphp-websocket/internal/cluster"
	"github.com/dunglas/frankenphp"
	"go.uber.org/zap"
)

func init() {
	frankenphp.RegisterExtension(unsafe.Pointer(&C.cluster_cli_module_entry))
}

// participant is this process as a CLI participant, nil in a server.
var participant atomic.Pointer[cliParticipant]

// cliParticipant is a `frankenphp php-cli` process in the cluster: it dials the
// members but accepts no connections, and its PHP takes the messages of the
// other members with cluster_receive().
type cliParticipant struct {
	member    *cluster.Membership
	logger    *zap.Logger
	inbox     chan pendingMessage
	receiving sync.Once

	mu  sync.Mutex
	bus *cluster.Bus
}

// pendingMessage is a message waiting for cluster_receive(), with how to
// answer it when it is a request.
type pendingMessage struct {
	message cluster.Message
	respond func([]byte)
}

func newCLIParticipant(member *cluster.Membership, logger *zap.Logger, queueSize int) *cliParticipant {
	if queueSize <= 0 {
		queueSize = cluster.DefaultQueueSize
	}

	return &cliParticipant{member: member, logger: logger, inbox: make(chan pendingMessage, queueSize)}
}

// startReceiving makes the member's messages wait for cluster_receive() from
// its first call on, which announces that it receives.
func (p *cliParticipant) startReceiving() {
	p.receiving.Do(func() { p.member.Handle(cluster.MessagesChannel, p.handle) })
}

// handle queues a message for cluster_receive(); with the queue full, the
// message is missed, as a member that does not keep up misses messages.
func (p *cliParticipant) handle(message cluster.Message, respond func([]byte)) {
	select {
	case p.inbox <- pendingMessage{message: message, respond: respond}:
	default:
		p.logger.Warn("cluster: a message for cluster_receive() was dropped, its queue is full", zap.String("from", message.From))
	}
}

// startCLI makes member a CLI participant when the environment names a
// configuration, and returns it. It joins on first use, so a command that
// never uses the cluster never joins it.
func startCLI(member *cluster.Membership, logger *zap.Logger) *cliParticipant {
	path := os.Getenv(cluster.CLIConfigEnv)
	if path == "" {
		return nil
	}

	config, err := cluster.LoadCLIConfig(path)
	if err != nil {
		logger.Error("cluster: not joining as a CLI participant", zap.String("config", path), zap.Error(err))

		return nil
	}

	queueSize := 0
	if config.Member != nil {
		queueSize = config.Member.QueueSize
	}
	p := newCLIParticipant(member, logger, queueSize)
	member.JoinOnFirstUse(func() {
		b, err := cluster.JoinCLI(member, config, logger)
		if err != nil {
			logger.Error("cluster: joining as a CLI participant failed", zap.String("config", path), zap.Error(err))

			return
		}
		p.mu.Lock()
		p.bus = b
		p.mu.Unlock()
	})

	return p
}

// leave says goodbye to the other members and gives what is queued for them
// the time the bus allows to be written.
func (p *cliParticipant) leave() {
	p.mu.Lock()
	b := p.bus
	p.bus = nil
	p.mu.Unlock()

	if b == nil {
		return
	}
	p.member.Leave(b)
	if err := b.Close(); err != nil {
		p.logger.Error("cluster: closing the bus of the CLI participant failed", zap.Error(err))
	}
}

// receive waits up to timeout for the next message.
func (p *cliParticipant) receive(timeout time.Duration) (pendingMessage, int64) {
	if p.member.Bus() == nil {
		return pendingMessage{}, CLUSTER_ERROR_NOT_JOINED
	}
	p.startReceiving()

	select {
	case pending := <-p.inbox:
		return pending, CLUSTER_ERROR_NONE
	default:
	}
	if timeout <= 0 {
		return pendingMessage{}, CLUSTER_ERROR_TIMEOUT
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case pending := <-p.inbox:
		return pending, CLUSTER_ERROR_NONE
	case <-timer.C:
		return pendingMessage{}, CLUSTER_ERROR_TIMEOUT
	}
}

//export go_cluster_cli_startup
func go_cluster_cli_startup() {
	if p := startCLI(cluster.Local, caddy.Log()); p != nil {
		participant.Store(p)
	}
}

//export go_cluster_cli_shutdown
func go_cluster_cli_shutdown() {
	if p := participant.Load(); p != nil {
		p.leave()
	}
}

// go_cluster_receive waits up to timeout milliseconds for the next message
// and returns its event, with a handle that go_cluster_answer releases.
//
//export go_cluster_receive
func go_cluster_receive(timeout C.long) (unsafe.Pointer, C.uintptr_t, C.int) {
	p := participant.Load()
	if p == nil {
		return nil, 0, C.CLUSTER_RECEIVE_UNAVAILABLE
	}
	if timeout < 0 {
		setLastError(CLUSTER_ERROR_INVALID_ARGUMENT)

		return nil, 0, C.CLUSTER_RECEIVE_NONE
	}

	pending, code := p.receive(time.Duration(timeout) * time.Millisecond)
	setLastError(code)
	if code != CLUSTER_ERROR_NONE {
		return nil, 0, C.CLUSTER_RECEIVE_NONE
	}

	event := frankenphp.PHPAssociativeArray(messageEvent(pending.message))

	return event, C.uintptr_t(cgo.NewHandle(pending)), C.CLUSTER_RECEIVE_MESSAGE
}

// go_cluster_answer answers the request of the handle with what the handler
// returned, and releases the handle. It reports whether a request went
// unanswered because the handler returned neither a string nor null.
//
//export go_cluster_answer
func go_cluster_answer(handle C.uintptr_t, answer unsafe.Pointer, outcome C.int) C.int {
	h := cgo.Handle(handle)
	pending := h.Value().(pendingMessage)
	h.Delete()

	if !pending.message.Request {
		return 0
	}

	switch outcome {
	case C.CLUSTER_ANSWER_GIVEN:
		pending.respond([]byte(frankenphp.GoString(answer)))
	case C.CLUSTER_ANSWER_INVALID:
		return 1
	}

	return 0
}
