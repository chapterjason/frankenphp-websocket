package websocket

import (
	"fmt"
	"path/filepath"
	"sync"

	"github.com/dunglas/frankenphp"
	frankenphpcaddy "github.com/dunglas/frankenphp/caddy"
)

// registerWorkers is FrankenPHP's registration of extension workers.
var registerWorkers = frankenphpcaddy.RegisterWorkers

type sharedWorker struct {
	num     int
	workers frankenphp.Workers
}

var (
	sharedWorkersMu sync.Mutex
	// sharedWorkers are the workers registered for each configuration, by the
	// FrankenPHP app of the configuration and the absolute path of the script.
	sharedWorkers = make(map[any]map[string]*sharedWorker)
)

// workersFor registers the worker script once per configuration: FrankenPHP
// refuses two workers of the same file, and directives on several servers,
// such as one site on HTTP and HTTPS, share it. It reports whether it
// registered the script, so the caller forgets the configuration's workers
// once it ends.
func workersFor(config any, file string, num int) (frankenphp.Workers, bool, error) {
	path, err := filepath.Abs(file)
	if err != nil {
		return nil, false, err
	}

	sharedWorkersMu.Lock()
	defer sharedWorkersMu.Unlock()

	workers := sharedWorkers[config]
	if workers == nil {
		workers = make(map[string]*sharedWorker)
		sharedWorkers[config] = workers
	}

	if shared, ok := workers[path]; ok {
		if shared.num != num {
			return nil, false, fmt.Errorf("worker %s is configured with %d and %d threads; use the same number everywhere", file, shared.num, num)
		}

		return shared.workers, false, nil
	}

	shared := &sharedWorker{num: num, workers: registerWorkers("websocket:"+path, file, num)}
	workers[path] = shared

	return shared.workers, true, nil
}

// forgetWorkers drops what workersFor registered for a configuration.
func forgetWorkers(config any) {
	sharedWorkersMu.Lock()
	defer sharedWorkersMu.Unlock()

	delete(sharedWorkers, config)
}
