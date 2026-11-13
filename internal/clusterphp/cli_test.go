package clusterphp

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/chapterjason/frankenphp-websocket/internal/cluster"
	"go.uber.org/zap/zaptest"
)

// run hands a command to the PHP worker directly, without the cluster.
func run(t *testing.T, command string) string {
	t.Helper()

	reply, err := testWorkers.SendMessage(context.Background(), messageEvent(cluster.Message{Data: []byte(command), Request: true}), nil)
	if err != nil {
		t.Fatalf("command %q: %v", command, err)
	}
	answer, ok := reply.(string)
	if !ok {
		t.Fatalf("command %q answered %T, want a string", command, reply)
	}

	return answer
}

type receiveResult struct {
	Handled bool   `json:"handled"`
	Error   int64  `json:"error"`
	Thrown  string `json:"thrown"`
	Warning string `json:"warning"`
	Event   *struct {
		Event   string `json:"event"`
		From    string `json:"from"`
		Data    string `json:"data"`
		Request bool   `json:"request"`
	} `json:"event"`
}

func receive(t *testing.T, mode string, timeout int) receiveResult {
	t.Helper()

	var result receiveResult
	answer := run(t, "receive:"+mode+":"+strconv.Itoa(timeout))
	if err := json.Unmarshal([]byte(answer), &result); err != nil {
		t.Fatalf("cluster_receive() result %q: %v", answer, err)
	}

	return result
}

func writeConfig(t *testing.T, name, content string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	return path
}

func waitFor(t *testing.T, what string, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting until %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// startCLIParticipant makes this process a CLI participant in a cluster with
// a Go member, which it returns.
func startCLIParticipant(t *testing.T) *cluster.Membership {
	t.Helper()

	other, _ := startCluster(t)
	p := newCLIParticipant(cluster.Local, zaptest.NewLogger(t), 0)
	participant.Store(p)
	t.Cleanup(func() { participant.Store(nil) })
	p.startReceiving()

	return other
}

func TestReceiveIsForCLIParticipants(t *testing.T) {
	startCluster(t)

	result := receive(t, "answer", 0)
	if !strings.HasPrefix(result.Thrown, "Error: cluster_receive() is for CLI participants") {
		t.Fatalf("cluster_receive() in a server = %+v, want an Error", result)
	}
}

func TestReceiveAnswersRequests(t *testing.T) {
	other := startCLIParticipant(t)

	answers := make(chan string, 1)
	go func() {
		answer, err := other.Request(cluster.Local.ID(), cluster.MessagesChannel, []byte("question"), 5*time.Second)
		if err != nil {
			answers <- "error: " + err.Error()

			return
		}
		answers <- string(answer)
	}()

	result := receive(t, "answer", 5000)
	if !result.Handled || result.Error != CLUSTER_ERROR_NONE || result.Event == nil ||
		result.Event.Event != "message" || result.Event.From != other.ID() || result.Event.Data != "question" || !result.Event.Request {
		t.Fatalf("cluster_receive() = %+v, want the request of the Go member", result)
	}
	if answer := <-answers; answer != "cli:question" {
		t.Fatalf("the Go member got %q, want the handler's answer", answer)
	}
}

func TestReceiveTakesMessages(t *testing.T) {
	other := startCLIParticipant(t)

	if err := other.Send(cluster.Local.ID(), cluster.MessagesChannel, []byte("note")); err != nil {
		t.Fatal(err)
	}
	result := receive(t, "int", 5000)
	if !result.Handled || result.Event == nil || result.Event.Data != "note" || result.Event.Request || result.Warning != "" {
		t.Fatalf("cluster_receive() = %+v, want the message, its return value ignored", result)
	}

	if result := receive(t, "answer", 0); result.Handled || result.Error != CLUSTER_ERROR_TIMEOUT || result.Event != nil {
		t.Fatalf("cluster_receive() without a message = %+v, want the timeout", result)
	}
	started := time.Now()
	if result := receive(t, "answer", 100); result.Handled || result.Error != CLUSTER_ERROR_TIMEOUT {
		t.Fatalf("cluster_receive() without a message = %+v, want the timeout", result)
	}
	if elapsed := time.Since(started); elapsed < 100*time.Millisecond {
		t.Fatalf("cluster_receive() returned after %v, want it to wait 100ms", elapsed)
	}
	if result := receive(t, "answer", -1); result.Handled || result.Error != CLUSTER_ERROR_INVALID_ARGUMENT {
		t.Fatalf("cluster_receive() with a negative timeout = %+v, want the invalid argument", result)
	}
}

func TestReceiveLeavesARequestUnansweredWhenTheHandlerFails(t *testing.T) {
	other := startCLIParticipant(t)

	for mode, check := range map[string]func(receiveResult) bool{
		"int": func(r receiveResult) bool {
			return r.Handled && strings.Contains(r.Warning, "must be a string or null, int given")
		},
		"throw": func(r receiveResult) bool { return r.Thrown == "RuntimeException: the handler failed" },
	} {
		failed := make(chan error, 1)
		go func() {
			_, err := other.Request(cluster.Local.ID(), cluster.MessagesChannel, []byte(mode), 300*time.Millisecond)
			failed <- err
		}()

		if result := receive(t, mode, 5000); !check(result) || result.Event == nil || result.Event.Data != mode {
			t.Fatalf("cluster_receive() with a handler that %s = %+v", mode, result)
		}
		if err := <-failed; !errors.Is(err, cluster.ErrTimeout) {
			t.Fatalf("request with a handler that %s = %v, want it unanswered", mode, err)
		}
	}
}

func TestReceiveAnswersNullAsAnEmptyString(t *testing.T) {
	other := startCLIParticipant(t)

	answers := make(chan []byte, 1)
	go func() {
		answer, err := other.Request(cluster.Local.ID(), cluster.MessagesChannel, []byte("x"), 5*time.Second)
		if err != nil {
			t.Error(err)
		}
		answers <- answer
	}()

	if result := receive(t, "null", 5000); !result.Handled {
		t.Fatalf("cluster_receive() = %+v, want the request handled", result)
	}
	if answer := <-answers; len(answer) != 0 {
		t.Fatalf("a null answer arrived as %q, want an empty string", answer)
	}
}

func TestStartCLIJoinsOnFirstUseAndLeaves(t *testing.T) {
	server := cluster.NewMembership()
	serverBus := join(t, server, &cluster.Mesh{Listen: "127.0.0.1:0", Secret: testSecret})

	config := writeConfig(t, "Caddyfile", "{\n\tcluster mesh {\n\t\tlisten :7070\n\t\tpeers "+serverBus.MeshAddr()+"\n\t\tsecret {env.TEST_CLUSTER_SECRET}\n\t}\n}\n")
	t.Setenv("TEST_CLUSTER_SECRET", testSecret)
	t.Setenv(cluster.CLIConfigEnv, config)

	member := cluster.NewMembership()
	p := startCLI(member, zaptest.NewLogger(t))
	if p == nil {
		t.Fatal("startCLI did not make a CLI participant")
	}
	t.Cleanup(p.leave)

	time.Sleep(100 * time.Millisecond)
	if slices.Contains(server.Members(), member.ID()) {
		t.Fatal("the CLI participant joined before its first use")
	}

	if member.Bus() == nil {
		t.Fatal("the CLI participant did not join on first use")
	}
	waitFor(t, "the server sees the CLI participant", func() bool { return slices.Contains(server.Members(), member.ID()) })

	p.leave()
	waitFor(t, "the server forgets the CLI participant", func() bool { return !slices.Contains(server.Members(), member.ID()) })
	if member.Bus() != nil {
		t.Fatal("the CLI participant is still a member after leaving")
	}
}

func TestStartCLINeedsAUsableConfiguration(t *testing.T) {
	t.Setenv(cluster.CLIConfigEnv, "")
	if p := startCLI(cluster.NewMembership(), zaptest.NewLogger(t)); p != nil {
		t.Fatal("a CLI participant without a configuration")
	}

	t.Setenv(cluster.CLIConfigEnv, writeConfig(t, "Caddyfile", "{\n\tcluster member {\n\t\tqueue_size 10\n\t}\n}\n"))
	if p := startCLI(cluster.NewMembership(), zaptest.NewLogger(t)); p != nil {
		t.Fatal("a CLI participant without a mesh or an uplink")
	}
}

func TestACLIParticipantReceivesFromItsFirstCallOn(t *testing.T) {
	other, _ := startCluster(t)
	cluster.Local.Handle(cluster.MessagesChannel, nil)
	p := newCLIParticipant(cluster.Local, zaptest.NewLogger(t), 0)
	participant.Store(p)
	t.Cleanup(func() { participant.Store(nil) })

	waitFor(t, "the member is known not to receive", func() bool {
		return !other.MembersInfo()[cluster.Local.ID()].Capabilities.Has(cluster.Receives)
	})
	if _, err := other.Request(cluster.Local.ID(), cluster.MessagesChannel, []byte("too early"), 5*time.Second); !errors.Is(err, cluster.ErrNoReceiver) {
		t.Fatalf("request before cluster_receive() = %v, want no receiver", err)
	}

	if result := receive(t, "answer", 0); result.Handled || result.Error != CLUSTER_ERROR_TIMEOUT {
		t.Fatalf("the first cluster_receive() = %+v, want nothing yet", result)
	}
	waitFor(t, "the member is known to receive", func() bool {
		return other.MembersInfo()[cluster.Local.ID()].Capabilities.Has(cluster.Receives)
	})
	if err := other.Send(cluster.Local.ID(), cluster.MessagesChannel, []byte("now")); err != nil {
		t.Fatal(err)
	}
	if result := receive(t, "answer", 5000); !result.Handled || result.Event == nil || result.Event.Data != "now" {
		t.Fatalf("cluster_receive() = %+v, want the message sent once it receives", result)
	}
}
