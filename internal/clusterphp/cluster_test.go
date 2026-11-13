package clusterphp

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/chapterjason/frankenphp-websocket/internal/cluster"
	"github.com/dunglas/frankenphp"
	"go.uber.org/zap/zaptest"
)

var testWorkers frankenphp.Workers

func TestMain(m *testing.M) {
	workers, opt := frankenphp.WithExtensionWorkers("cluster-test", "testdata/worker.php", 2)
	if err := frankenphp.Init(opt); err != nil {
		fmt.Fprintln(os.Stderr, "frankenphp init:", err)
		os.Exit(1)
	}

	testWorkers = workers
	code := m.Run()
	frankenphp.Shutdown()
	os.Exit(code)
}

const testSecret = "test secret"

func join(t *testing.T, m *cluster.Membership, mesh *cluster.Mesh) *cluster.Bus {
	t.Helper()

	b, err := cluster.NewBus(nil, 0, mesh, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Join(b, cluster.Settings{QueryTimeout: time.Second}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		m.Leave(b)
		_ = b.Close()
	})

	return b
}

// inbox records what a Go member gets on the PHP channel, and answers
// requests with "go:" and the data.
type inbox struct {
	mu       sync.Mutex
	messages []cluster.Message
}

func (i *inbox) handle(message cluster.Message, respond func([]byte)) {
	i.mu.Lock()
	i.messages = append(i.messages, message)
	i.mu.Unlock()

	respond(append([]byte("go:"), message.Data...))
}

func (i *inbox) waitFor(t *testing.T, data string) cluster.Message {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		i.mu.Lock()
		for _, message := range i.messages {
			if string(message.Data) == data {
				i.mu.Unlock()

				return message
			}
		}
		i.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("the Go member never got %q", data)

	return cluster.Message{}
}

// startCluster joins this process, whose PHP worker takes the PHP channel,
// and a Go member to each other.
func startCluster(t *testing.T) (*cluster.Membership, *inbox) {
	t.Helper()

	cluster.Local.Handle(cluster.MessagesChannel, handler(testWorkers, zaptest.NewLogger(t)))
	local := join(t, cluster.Local, &cluster.Mesh{Listen: "127.0.0.1:0", Secret: testSecret})

	other := cluster.NewMembership()
	var got inbox
	other.Handle(cluster.MessagesChannel, got.handle)
	join(t, other, &cluster.Mesh{Peers: []string{local.MeshAddr()}, Secret: testSecret})

	deadline := time.Now().Add(5 * time.Second)
	for !slices.Contains(cluster.Local.Members(), other.ID()) || !slices.Contains(other.Members(), cluster.Local.ID()) {
		if time.Now().After(deadline) {
			t.Fatal("the members never saw each other")
		}
		time.Sleep(10 * time.Millisecond)
	}

	return other, &got
}

// ask sends a command to the PHP worker of this process and returns its answer.
func ask(t *testing.T, other *cluster.Membership, command string) string {
	t.Helper()

	answer, err := other.Request(cluster.Local.ID(), cluster.MessagesChannel, []byte(command), 5*time.Second)
	if err != nil {
		t.Fatalf("request %q: %v", command, err)
	}

	return string(answer)
}

func TestTheWorkerGetsMessagesAndAnswersRequests(t *testing.T) {
	other, _ := startCluster(t)

	var event struct {
		Data    string `json:"data"`
		From    string `json:"from"`
		Request bool   `json:"request"`
	}
	if err := json.Unmarshal([]byte(ask(t, other, "echo:hello")), &event); err != nil {
		t.Fatal(err)
	}
	if event.Data != "hello" || event.From != other.ID() || !event.Request {
		t.Fatalf("the worker saw %+v, want the request from the Go member", event)
	}

	if answer := ask(t, other, "null"); answer != "" {
		t.Fatalf("a null answer arrived as %q, want an empty string", answer)
	}
}

func TestPHPKnowsItselfAndTheMembers(t *testing.T) {
	other, _ := startCluster(t)

	if self := ask(t, other, "self"); self != cluster.Local.ID() {
		t.Fatalf("cluster_self() = %q, want %q", self, cluster.Local.ID())
	}

	var members []string
	if err := json.Unmarshal([]byte(ask(t, other, "members")), &members); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(members, []string{other.ID()}) {
		t.Fatalf("cluster_members() = %v, want the Go member", members)
	}
}

func TestPHPSendsPublishesAndRequests(t *testing.T) {
	other, got := startCluster(t)

	if result := ask(t, other, "send:"+other.ID()+":sent by PHP"); result != `[true,0]` {
		t.Fatalf("cluster_send() = %s, want true and no error", result)
	}
	if message := got.waitFor(t, "sent by PHP"); message.From != cluster.Local.ID() || message.Request {
		t.Fatalf("the Go member got %+v, want a message from PHP", message)
	}

	if result := ask(t, other, "publish:published by PHP"); result != `[1,0]` {
		t.Fatalf("cluster_publish() = %s, want queued for the one member", result)
	}
	got.waitFor(t, "published by PHP")

	if result := ask(t, other, "request:"+other.ID()+":0:asked by PHP"); result != `["go:asked by PHP",0]` {
		t.Fatalf("cluster_request() = %s, want the Go member's answer", result)
	}

	silent := cluster.NewMembership()
	join(t, silent, &cluster.Mesh{Peers: []string{cluster.Local.Bus().MeshAddr()}, Secret: testSecret})
	noReceiver := strconv.Itoa(CLUSTER_ERROR_NO_RECEIVER)
	if result := ask(t, other, "request:"+silent.ID()+":2000:nobody answers"); result != `["",`+noReceiver+`]` {
		t.Fatalf("cluster_request() to a member without a worker = %s, want an empty answer and no receiver", result)
	}
	if result := ask(t, other, "send:"+silent.ID()+":nobody takes it"); result != `[false,`+noReceiver+`]` {
		t.Fatalf("cluster_send() to a member without a worker = %s, want false and no receiver", result)
	}
	if result := ask(t, other, "publish:to those that receive"); result != `[1,0]` {
		t.Fatalf("cluster_publish() = %s, want queued for the one member that receives", result)
	}

	var info map[string]struct {
		Role     string `json:"role"`
		Receives bool   `json:"receives"`
	}
	if err := json.Unmarshal([]byte(ask(t, other, "member-info")), &info); err != nil {
		t.Fatal(err)
	}
	if len(info) != 2 || info[other.ID()].Role != "server" || !info[other.ID()].Receives || info[silent.ID()].Receives {
		t.Fatalf("cluster_member_info() = %+v, want the Go member receiving and the silent one not", info)
	}

	invalid := strconv.Itoa(CLUSTER_ERROR_INVALID_ARGUMENT)
	if result := ask(t, other, "send::no instance"); result != `[false,`+invalid+`]` {
		t.Fatalf("cluster_send() without an instance = %s, want the invalid argument", result)
	}
	if result := ask(t, other, "request:"+other.ID()+":-1:x"); result != `["",`+invalid+`]` {
		t.Fatalf("cluster_request() with a negative timeout = %s, want the invalid argument", result)
	}
}

func TestSelfAndMembersClearTheLastError(t *testing.T) {
	other, _ := startCluster(t)

	silent := cluster.NewMembership()
	join(t, silent, &cluster.Mesh{Peers: []string{cluster.Local.Bus().MeshAddr()}, Secret: testSecret})
	deadline := time.Now().Add(5 * time.Second)
	for !slices.Contains(cluster.Local.Members(), silent.ID()) {
		if time.Now().After(deadline) {
			t.Fatal("the silent member was never seen")
		}
		time.Sleep(10 * time.Millisecond)
	}

	want := `[` + strconv.Itoa(CLUSTER_ERROR_NO_RECEIVER) + `,` + strconv.Itoa(CLUSTER_ERROR_NONE) + `]`
	for _, call := range []string{"members", "self", "member-info"} {
		if result := ask(t, other, "after-error:"+silent.ID()+":"+call); result != want {
			t.Fatalf("cluster_%s() after a failed request = %s, want the error cleared", call, result)
		}
	}
}

func TestTheWorkerGetsLeftEvents(t *testing.T) {
	_, got := startCluster(t)

	leftHandler(testWorkers, zaptest.NewLogger(t))(cluster.MemberLeft{Instance: "GONE", Reason: cluster.ReasonTimeout})

	want := `left:{"event":"left","instance":"GONE","reason":"timeout","role":"server"}`
	if message := got.waitFor(t, want); message.From != cluster.Local.ID() {
		t.Fatalf("the left event came back from %s, want the worker of this member", message.From)
	}
}

func TestErrorCodes(t *testing.T) {
	tests := map[error]int64{
		nil:                   CLUSTER_ERROR_NONE,
		cluster.ErrNotJoined:  CLUSTER_ERROR_NOT_JOINED,
		cluster.ErrTimeout:    CLUSTER_ERROR_TIMEOUT,
		cluster.ErrNoReceiver: CLUSTER_ERROR_NO_RECEIVER,
		cluster.ErrBusFailed:  CLUSTER_ERROR_BUS_FAILED,
		errors.New("unknown"): CLUSTER_ERROR_BUS_FAILED,
	}

	for err, want := range tests {
		if got := errorCode(err); got != want {
			t.Errorf("errorCode(%v) = %d, want %d", err, got, want)
		}
	}
}
