package websocket

import (
	"context"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dunglas/frankenphp"
)

type fakeWorkers struct{ file string }

func (fakeWorkers) SendRequest(http.ResponseWriter, *http.Request) error { return nil }

func (fakeWorkers) SendMessage(context.Context, any, http.ResponseWriter) (any, error) {
	return nil, nil
}

func (fakeWorkers) NumThreads() int { return 1 }

// countRegistrations replaces FrankenPHP's registration for the test and
// returns how many times each script was registered.
func countRegistrations(t *testing.T) map[string]int {
	t.Helper()

	counts := make(map[string]int)
	previous := registerWorkers
	registerWorkers = func(_ string, file string, _ int, _ ...frankenphp.WorkerOption) frankenphp.Workers {
		path, _ := filepath.Abs(file)
		counts[path]++

		return fakeWorkers{file: path}
	}
	t.Cleanup(func() { registerWorkers = previous })

	return counts
}

func TestAWorkerIsRegisteredOncePerConfiguration(t *testing.T) {
	counts := countRegistrations(t)
	config, otherConfig := new(int), new(int)
	t.Cleanup(func() {
		forgetWorkers(config)
		forgetWorkers(otherConfig)
	})

	absolute, err := filepath.Abs("worker.php")
	if err != nil {
		t.Fatal(err)
	}

	// One site served over HTTPS and HTTP: a directive on each server.
	first, registered, err := workersFor(config, "worker.php", 2)
	if err != nil || !registered {
		t.Fatalf("first directive: registered %v, err %v; want it registered", registered, err)
	}
	second, registered, err := workersFor(config, absolute, 2)
	if err != nil || registered || second != first {
		t.Fatalf("second directive: registered %v, err %v, same workers %v; want the first's workers shared", registered, err, second == first)
	}
	if counts[absolute] != 1 {
		t.Fatalf("the script was registered %d times, want once", counts[absolute])
	}

	if _, _, err := workersFor(config, "worker.php", 4); err == nil || !strings.Contains(err.Error(), "2 and 4 threads") {
		t.Fatalf("err = %v, want the different numbers of threads explained", err)
	}

	if _, registered, _ := workersFor(otherConfig, "worker.php", 2); !registered {
		t.Fatal("another configuration did not register its own workers")
	}

	forgetWorkers(config)
	if _, registered, _ := workersFor(config, "worker.php", 2); !registered {
		t.Fatal("a configuration that ended still held its workers")
	}
	if counts[absolute] != 3 {
		t.Fatalf("the script was registered %d times, want once for each of the 3 configurations", counts[absolute])
	}
}
