package websocket

import (
	"maps"
	"net/http"
	"slices"
	"strings"
	"sync"

	"github.com/dunglas/frankenphp"
)

// session is what PHP can address by connection ID: it exists from the
// handshake on, gets its connection once the handshake is accepted, and
// carries the attributes and tags PHP gives it.
type session struct {
	id      string
	request frankenphp.AssociativeArray[any]

	mu         sync.Mutex
	conn       *conn
	attributes map[string]string

	// removed is closed once the hub forgets the session, after its close event.
	removed chan struct{}
}

func newSession(id string, r *http.Request) *session {
	return &session{id: id, request: requestContext(r), attributes: make(map[string]string), removed: make(chan struct{})}
}

// attached returns the connection once the handshake was accepted, open or not.
func (s *session) attached() *conn {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.conn
}

func (s *session) attach(c *conn) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.conn = c
}

// openConn returns the connection while messages can be sent to it.
func (s *session) openConn() *conn {
	s.mu.Lock()
	c := s.conn
	s.mu.Unlock()

	if c == nil || !c.open() {
		return nil
	}

	return c
}

func (s *session) setAttribute(name, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.attributes[name] = value
}

func (s *session) removeAttribute(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.attributes, name)
}

func (s *session) attributesMap() map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return maps.Clone(s.attributes)
}

func (s *session) attributesArray() frankenphp.AssociativeArray[any] {
	s.mu.Lock()
	defer s.mu.Unlock()

	names := slices.Sorted(maps.Keys(s.attributes))
	array := frankenphp.AssociativeArray[any]{Map: make(map[string]any, len(names)), Order: names}
	for _, name := range names {
		array.Map[name] = s.attributes[name]
	}

	return array
}

// requestContext is the handshake request as PHP saw it, for the events.
func requestContext(r *http.Request) frankenphp.AssociativeArray[any] {
	names := make([]string, 0, len(r.Header))
	for name := range r.Header {
		names = append(names, strings.ToLower(name))
	}
	slices.Sort(names)

	headers := frankenphp.AssociativeArray[any]{Map: make(map[string]any, len(names)), Order: names}
	for _, name := range names {
		separator := ", "
		if name == "cookie" {
			separator = "; "
		}
		headers.Map[name] = strings.Join(r.Header.Values(name), separator)
	}

	return newEvent(
		"method", r.Method,
		"uri", r.URL.RequestURI(),
		"protocol", r.Proto,
		"remote_addr", r.RemoteAddr,
		"headers", headers,
	)
}

// hub indexes the sessions of every handler, so PHP can address a connection
// from any worker or request.
type hub struct {
	mu       sync.Mutex
	sessions map[string]*session
	tags     map[string]map[string]*session
	tagsOf   map[string]map[string]struct{}
}

func newHub() *hub {
	return &hub{
		sessions: make(map[string]*session),
		tags:     make(map[string]map[string]*session),
		tagsOf:   make(map[string]map[string]struct{}),
	}
}

func (h *hub) add(s *session) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.sessions[s.id] = s
}

func (h *hub) remove(id string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if s, ok := h.sessions[id]; ok {
		close(s.removed)
	}
	for tag := range h.tagsOf[id] {
		delete(h.tags[tag], id)
		if len(h.tags[tag]) == 0 {
			delete(h.tags, tag)
		}
	}
	delete(h.tagsOf, id)
	delete(h.sessions, id)
}

func (h *hub) get(id string) (*session, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()

	s, ok := h.sessions[id]

	return s, ok
}

func (h *hub) all() []*session {
	h.mu.Lock()
	defer h.mu.Unlock()

	return slices.Collect(maps.Values(h.sessions))
}

func (h *hub) tag(id, tag string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()

	s, ok := h.sessions[id]
	if !ok {
		return false
	}

	if h.tags[tag] == nil {
		h.tags[tag] = make(map[string]*session)
	}
	h.tags[tag][id] = s

	if h.tagsOf[id] == nil {
		h.tagsOf[id] = make(map[string]struct{})
	}
	h.tagsOf[id][tag] = struct{}{}

	return true
}

func (h *hub) untag(id, tag string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()

	if _, ok := h.sessions[id]; !ok {
		return false
	}

	delete(h.tags[tag], id)
	if len(h.tags[tag]) == 0 {
		delete(h.tags, tag)
	}
	delete(h.tagsOf[id], tag)

	return true
}

func (h *hub) tagged(tag string) []*session {
	h.mu.Lock()
	defer h.mu.Unlock()

	return slices.Collect(maps.Values(h.tags[tag]))
}

func (h *hub) tagList(id string) []string {
	h.mu.Lock()
	defer h.mu.Unlock()

	return slices.Sorted(maps.Keys(h.tagsOf[id]))
}

func (h *hub) tagsArray(id string) []any {
	h.mu.Lock()
	defer h.mu.Unlock()

	tags := slices.Sorted(maps.Keys(h.tagsOf[id]))
	array := make([]any, len(tags))
	for i, tag := range tags {
		array[i] = tag
	}

	return array
}
