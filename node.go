package frankenphpwebsocket

import (
	"crypto/rand"
	"encoding/json"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gobwas/ws"
)

// Bus connects instances so a push reaches a connection held by another
// instance. It carries opaque payloads; delivery is at most once.
type Bus interface {
	// Open joins the bus as the given instance. deliver receives every payload
	// published to that instance or to all instances, possibly including the
	// instance's own payloads to all; it must not block for long.
	Open(instance string, deliver func(payload []byte)) error
	// Publish sends payload to one instance, or to all instances when instance
	// is empty. Publishing to an instance that is gone is not an error.
	Publish(instance string, payload []byte) error
	Close() error
}

const (
	defaultQueryTimeout     = time.Second
	defaultPresenceInterval = 2 * time.Second
	// A peer that missed this many presence intervals counts as gone.
	presenceMisses = 3
)

// Kinds of bus messages. Pushes, queries and presence travel the same bus; a
// message with a request ID expects a reply to its origin.
const (
	busSend            = "send"
	busPing            = "ping"
	busClose           = "close"
	busTag             = "tag"
	busBroadcast       = "broadcast"
	busIsOpen          = "is_open"
	busAttributes      = "attributes"
	busSetAttribute    = "set_attribute"
	busRemoveAttribute = "remove_attribute"
	busTags            = "tags"
	busAddTag          = "add_tag"
	busRemoveTag       = "remove_tag"
	busConnections     = "connections"
	busReply           = "reply"
	busHello           = "hello"
	busPresence        = "presence"
	busLeave           = "leave"
)

type busMessage struct {
	Origin     string `json:"origin"`
	Kind       string `json:"kind"`
	Request    uint64 `json:"request,omitempty"`
	Connection string `json:"connection,omitempty"`
	Tag        string `json:"tag,omitempty"`
	Name       string `json:"name,omitempty"`
	Value      string `json:"value,omitempty"`
	Data       []byte `json:"data,omitempty"`
	Text       bool   `json:"text,omitempty"`
	Code       int    `json:"code,omitempty"`
	Reason     string `json:"reason,omitempty"`

	Error       int64             `json:"error,omitempty"`
	Count       int64             `json:"count,omitempty"`
	Attributes  map[string]string `json:"attributes,omitempty"`
	Tags        []string          `json:"tags,omitempty"`
	Connections []string          `json:"connections,omitempty"`
}

// node is one instance: the connections it holds and the bus that reaches the
// other instances.
// busSettings tune how an instance takes part in the cluster; zero values
// take the defaults.
type busSettings struct {
	queryTimeout     time.Duration
	presenceInterval time.Duration
}

type node struct {
	sessions *hub

	mu               sync.RWMutex
	id               string
	ownIDs           map[string]bool
	bus              Bus
	queryTimeout     time.Duration
	presenceInterval time.Duration
	stopPresence     chan struct{}

	peersMu sync.Mutex
	peers   map[string]time.Time

	pendingMu   sync.Mutex
	nextRequest uint64
	pending     map[uint64]chan busMessage
}

// local is the node of this process, the one PHP addresses.
var local = newNode()

func newNode() *node {
	id := newInstanceID()

	return &node{
		sessions:         newHub(),
		presenceInterval: defaultPresenceInterval,
		id:               id,
		ownIDs:           map[string]bool{id: true},
		queryTimeout:     defaultQueryTimeout,
		peers:            make(map[string]time.Time),
		pending:          make(map[uint64]chan busMessage),
	}
}

func newInstanceID() string {
	return rand.Text()[:13]
}

// connectionID names a new connection after the instance holding it, so other
// instances can route to it.
func (n *node) connectionID() string {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.id + "." + rand.Text()
}

// instanceOf names the instance holding a connection; an ID without one
// names none.
func instanceOf(connection string) string {
	instance, _, found := strings.Cut(connection, ".")
	if !found {
		return ""
	}

	return instance
}

// joinBus makes b the bus of n, under a new instance ID: connections do not
// survive a config reload, so the old and new bus never claim the same ID.
// The previous bus keeps delivering to connections named after it until it
// is closed, but leaves the membership.
func (n *node) joinBus(b Bus, settings busSettings) error {
	id := newInstanceID()
	if err := b.Open(id, func(payload []byte) { n.receive(b, payload) }); err != nil {
		return err
	}

	stop := make(chan struct{})

	n.mu.Lock()
	previous, previousID, previousStop := n.bus, n.id, n.stopPresence
	n.id, n.bus, n.stopPresence = id, b, stop
	n.ownIDs[id] = true
	n.queryTimeout = orDefault(settings.queryTimeout, defaultQueryTimeout)
	n.presenceInterval = orDefault(settings.presenceInterval, defaultPresenceInterval)
	interval := n.presenceInterval
	n.mu.Unlock()

	if previous != nil {
		close(previousStop)
		n.publish(previous, "", previousID, busMessage{Kind: busLeave})
	}

	n.publish(b, "", id, busMessage{Kind: busHello})
	go n.announce(b, id, interval, stop)

	return nil
}

func (n *node) leaveBus(b Bus) {
	n.mu.Lock()
	if n.bus != b {
		n.mu.Unlock()

		return
	}
	id, stop := n.id, n.stopPresence
	n.bus, n.stopPresence = nil, nil
	n.mu.Unlock()

	close(stop)
	n.publish(b, "", id, busMessage{Kind: busLeave})
}

func (n *node) announce(b Bus, id string, interval time.Duration, stop chan struct{}) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			n.publish(b, "", id, busMessage{Kind: busPresence})
		}
	}
}

func (n *node) current() (string, Bus) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.id, n.bus
}

func (n *node) timeout() time.Duration {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.queryTimeout
}

func (n *node) isOwnID(id string) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.ownIDs[id]
}

func (n *node) sawPeer(id string) {
	n.peersMu.Lock()
	defer n.peersMu.Unlock()

	n.peers[id] = time.Now()
}

func (n *node) forgetPeer(id string) {
	n.peersMu.Lock()
	defer n.peersMu.Unlock()

	delete(n.peers, id)
}

func (n *node) livePeers() []string {
	n.mu.RLock()
	interval := n.presenceInterval
	n.mu.RUnlock()

	n.peersMu.Lock()
	defer n.peersMu.Unlock()

	cutoff := time.Now().Add(-presenceMisses * interval)
	live := make([]string, 0, len(n.peers))
	for id, seen := range n.peers {
		if seen.Before(cutoff) {
			delete(n.peers, id)

			continue
		}
		live = append(live, id)
	}

	return live
}

func (n *node) localConn(connection string) (*conn, int64, bool) {
	s, ok := n.sessions.get(connection)
	if !ok {
		return nil, WEBSOCKET_ERROR_NOT_FOUND, false
	}

	c := s.openConn()
	if c == nil {
		return nil, WEBSOCKET_ERROR_NOT_OPEN, true
	}

	return c, WEBSOCKET_ERROR_NONE, true
}

// remote reports whether connection is not held here and names another
// instance a bus can reach; otherwise there is no such connection.
func (n *node) remote(connection string) bool {
	if _, held := n.sessions.get(connection); held {
		return false
	}

	instance := instanceOf(connection)
	self, bus := n.current()

	return bus != nil && instance != "" && instance != self && !n.isOwnID(instance)
}

func (n *node) send(connection string, text bool, data []byte) int64 {
	if n.remote(connection) {
		return n.publishTo(connection, busMessage{Kind: busSend, Connection: connection, Text: text, Data: data})
	}

	return n.sendLocal(connection, text, data)
}

func (n *node) sendLocal(connection string, text bool, data []byte) int64 {
	c, code, _ := n.localConn(connection)
	if c == nil {
		return code
	}

	if err := c.send(text, data); err != nil {
		return writeError(err)
	}

	return WEBSOCKET_ERROR_NONE
}

func (n *node) ping(connection string, payload []byte) int64 {
	if n.remote(connection) {
		return n.publishTo(connection, busMessage{Kind: busPing, Connection: connection, Data: payload})
	}

	c, code, _ := n.localConn(connection)
	if c == nil {
		return code
	}

	if err := c.ping(payload); err != nil {
		return writeError(err)
	}

	return WEBSOCKET_ERROR_NONE
}

func (n *node) close(connection string, code ws.StatusCode, reason string) int64 {
	if n.remote(connection) {
		return n.publishTo(connection, busMessage{Kind: busClose, Connection: connection, Code: int(code), Reason: reason})
	}

	return n.closeLocal(connection, code, reason)
}

func (n *node) closeLocal(connection string, code ws.StatusCode, reason string) int64 {
	c, errCode, _ := n.localConn(connection)
	if c == nil {
		return errCode
	}

	if err := c.startClose(code, reason); err != nil {
		return writeError(err)
	}
	go c.awaitClose()

	return WEBSOCKET_ERROR_NONE
}

func (n *node) isOpen(connection string) int64 {
	if n.remote(connection) {
		_, code := n.request(connection, busMessage{Kind: busIsOpen, Connection: connection})

		return code
	}

	_, code, _ := n.localConn(connection)

	return code
}

func (n *node) attributes(connection string) (map[string]string, int64) {
	if n.remote(connection) {
		reply, code := n.request(connection, busMessage{Kind: busAttributes, Connection: connection})

		return reply.Attributes, code
	}

	return n.attributesLocal(connection)
}

func (n *node) attributesLocal(connection string) (map[string]string, int64) {
	s, ok := n.sessions.get(connection)
	if !ok {
		return nil, WEBSOCKET_ERROR_NOT_FOUND
	}

	return s.attributesMap(), WEBSOCKET_ERROR_NONE
}

func (n *node) setAttribute(connection, name, value string) int64 {
	if n.remote(connection) {
		_, code := n.request(connection, busMessage{Kind: busSetAttribute, Connection: connection, Name: name, Value: value})

		return code
	}

	return n.setAttributeLocal(connection, name, value)
}

func (n *node) setAttributeLocal(connection, name, value string) int64 {
	s, ok := n.sessions.get(connection)
	if !ok {
		return WEBSOCKET_ERROR_NOT_FOUND
	}
	s.setAttribute(name, value)

	return WEBSOCKET_ERROR_NONE
}

func (n *node) removeAttribute(connection, name string) int64 {
	if n.remote(connection) {
		_, code := n.request(connection, busMessage{Kind: busRemoveAttribute, Connection: connection, Name: name})

		return code
	}

	return n.removeAttributeLocal(connection, name)
}

func (n *node) removeAttributeLocal(connection, name string) int64 {
	s, ok := n.sessions.get(connection)
	if !ok {
		return WEBSOCKET_ERROR_NOT_FOUND
	}
	s.removeAttribute(name)

	return WEBSOCKET_ERROR_NONE
}

func (n *node) tags(connection string) ([]string, int64) {
	if n.remote(connection) {
		reply, code := n.request(connection, busMessage{Kind: busTags, Connection: connection})

		return reply.Tags, code
	}

	return n.tagsLocal(connection)
}

func (n *node) tagsLocal(connection string) ([]string, int64) {
	if _, ok := n.sessions.get(connection); !ok {
		return nil, WEBSOCKET_ERROR_NOT_FOUND
	}

	return n.sessions.tagList(connection), WEBSOCKET_ERROR_NONE
}

func (n *node) addTag(connection, tag string) int64 {
	if n.remote(connection) {
		_, code := n.request(connection, busMessage{Kind: busAddTag, Connection: connection, Tag: tag})

		return code
	}

	return n.addTagLocal(connection, tag)
}

func (n *node) addTagLocal(connection, tag string) int64 {
	if !n.sessions.tag(connection, tag) {
		return WEBSOCKET_ERROR_NOT_FOUND
	}

	return WEBSOCKET_ERROR_NONE
}

func (n *node) removeTag(connection, tag string) int64 {
	if n.remote(connection) {
		_, code := n.request(connection, busMessage{Kind: busRemoveTag, Connection: connection, Tag: tag})

		return code
	}

	return n.removeTagLocal(connection, tag)
}

func (n *node) removeTagLocal(connection, tag string) int64 {
	if !n.sessions.untag(connection, tag) {
		return WEBSOCKET_ERROR_NOT_FOUND
	}

	return WEBSOCKET_ERROR_NONE
}

func (n *node) openConnections() []string {
	ids := make([]string, 0)
	for _, s := range n.sessions.all() {
		if s.openConn() != nil {
			ids = append(ids, s.id)
		}
	}

	return ids
}

// connections lists the open connections of every instance.
func (n *node) connections() ([]string, int64) {
	ids := n.openConnections()

	replies, code := n.gather(busMessage{Kind: busConnections})
	for _, reply := range replies {
		ids = append(ids, reply.Connections...)
	}
	slices.Sort(ids)

	return ids, code
}

// sendToTag sends to the tagged connections of every instance and returns to
// how many it was sent.
func (n *node) sendToTag(tag string, text bool, data []byte) (int64, int64) {
	sent := sendToAll(n.sessions.tagged(tag), text, data)

	replies, code := n.gather(busMessage{Kind: busTag, Tag: tag, Text: text, Data: data})
	for _, reply := range replies {
		sent += reply.Count
	}

	return sent, code
}

// broadcast sends to the connections of every instance and returns to how
// many it was sent.
func (n *node) broadcast(text bool, data []byte) (int64, int64) {
	sent := sendToAll(n.sessions.all(), text, data)

	replies, code := n.gather(busMessage{Kind: busBroadcast, Text: text, Data: data})
	for _, reply := range replies {
		sent += reply.Count
	}

	return sent, code
}

func sendToAll(targets []*session, text bool, data []byte) int64 {
	var (
		sent atomic.Int64
		wg   sync.WaitGroup
	)
	for _, s := range targets {
		c := s.openConn()
		if c == nil {
			continue
		}

		wg.Go(func() {
			if c.send(text, data) == nil {
				sent.Add(1)
			}
		})
	}
	wg.Wait()

	return sent.Load()
}

func (n *node) publishTo(connection string, msg busMessage) int64 {
	self, bus := n.current()
	if bus == nil {
		return WEBSOCKET_ERROR_NOT_FOUND
	}

	return n.publish(bus, instanceOf(connection), self, msg)
}

func (n *node) publish(bus Bus, instance, self string, msg busMessage) int64 {
	msg.Origin = self

	payload, err := json.Marshal(msg)
	if err != nil {
		return WEBSOCKET_ERROR_BUS_FAILED
	}

	if err := bus.Publish(instance, payload); err != nil {
		return WEBSOCKET_ERROR_BUS_FAILED
	}

	return WEBSOCKET_ERROR_NONE
}

func (n *node) openRequest(replies int) (uint64, chan busMessage) {
	n.pendingMu.Lock()
	defer n.pendingMu.Unlock()

	n.nextRequest++
	ch := make(chan busMessage, replies)
	n.pending[n.nextRequest] = ch

	return n.nextRequest, ch
}

func (n *node) closeRequest(id uint64) {
	n.pendingMu.Lock()
	defer n.pendingMu.Unlock()

	delete(n.pending, id)
}

func (n *node) deliverReply(msg busMessage) {
	n.pendingMu.Lock()
	ch, ok := n.pending[msg.Request]
	n.pendingMu.Unlock()

	if !ok {
		return
	}

	select {
	case ch <- msg:
	default:
	}
}

// request asks the instance holding connection and waits for its reply; the
// returned code is the reply's, or WEBSOCKET_ERROR_TIMEOUT.
func (n *node) request(connection string, msg busMessage) (busMessage, int64) {
	self, bus := n.current()
	if bus == nil {
		return busMessage{}, WEBSOCKET_ERROR_NOT_FOUND
	}

	id, replies := n.openRequest(1)
	defer n.closeRequest(id)

	msg.Request = id
	if code := n.publish(bus, instanceOf(connection), self, msg); code != WEBSOCKET_ERROR_NONE {
		return busMessage{}, code
	}

	timer := time.NewTimer(n.timeout())
	defer timer.Stop()

	select {
	case reply := <-replies:
		return reply, reply.Error
	case <-timer.C:
		return busMessage{}, WEBSOCKET_ERROR_TIMEOUT
	}
}

// gather asks every instance and waits for the replies of those known to be
// alive. Missing replies make the code WEBSOCKET_ERROR_TIMEOUT, unless the bus
// reported an error, which then is the code.
func (n *node) gather(msg busMessage) ([]busMessage, int64) {
	self, bus := n.current()
	if bus == nil {
		return nil, WEBSOCKET_ERROR_NONE
	}

	expected := n.livePeers()
	id, replies := n.openRequest(len(expected) + 16)
	defer n.closeRequest(id)

	// An error may mean only some instances missed the message, so the others'
	// replies still count.
	msg.Request = id
	publishCode := n.publish(bus, "", self, msg)

	missing := make(map[string]bool, len(expected))
	for _, peer := range expected {
		missing[peer] = true
	}

	timer := time.NewTimer(n.timeout())
	defer timer.Stop()

	var received []busMessage
	seen := make(map[string]bool)
	for len(missing) > 0 {
		select {
		case reply := <-replies:
			if seen[reply.Origin] {
				continue
			}
			seen[reply.Origin] = true
			delete(missing, reply.Origin)
			received = append(received, reply)
		case <-timer.C:
			if publishCode != WEBSOCKET_ERROR_NONE {
				return received, publishCode
			}

			return received, WEBSOCKET_ERROR_TIMEOUT
		}
	}

	return received, publishCode
}

func (n *node) reply(to busMessage, reply busMessage) {
	self, bus := n.current()
	if bus == nil || to.Request == 0 {
		return
	}

	reply.Kind = busReply
	reply.Request = to.Request
	n.publish(bus, to.Origin, self, reply)
}

// receive handles a payload from bus b, answering only for this instance. A
// message to all instances is taken only from the current bus, so it is not
// handled twice while a config reload runs the old and new bus side by side.
func (n *node) receive(b Bus, payload []byte) {
	var msg busMessage
	if err := json.Unmarshal(payload, &msg); err != nil {
		return
	}

	if n.isOwnID(msg.Origin) {
		return
	}

	if msg.Kind == busLeave {
		n.forgetPeer(msg.Origin)

		return
	}
	n.sawPeer(msg.Origin)

	_, current := n.current()
	fromCurrent := b == current

	switch msg.Kind {
	case busReply:
		n.deliverReply(msg)
	case busHello:
		if fromCurrent {
			self, _ := n.current()
			n.publish(b, msg.Origin, self, busMessage{Kind: busPresence})
		}
	case busPresence:
	case busSend:
		n.sendLocal(msg.Connection, msg.Text, msg.Data)
	case busPing:
		if c, _, _ := n.localConn(msg.Connection); c != nil {
			_ = c.ping(msg.Data)
		}
	case busClose:
		n.closeLocal(msg.Connection, ws.StatusCode(msg.Code), msg.Reason)
	case busTag:
		if fromCurrent {
			n.reply(msg, busMessage{Count: sendToAll(n.sessions.tagged(msg.Tag), msg.Text, msg.Data)})
		}
	case busBroadcast:
		if fromCurrent {
			n.reply(msg, busMessage{Count: sendToAll(n.sessions.all(), msg.Text, msg.Data)})
		}
	case busConnections:
		if fromCurrent {
			n.reply(msg, busMessage{Connections: n.openConnections()})
		}
	case busIsOpen:
		_, code, _ := n.localConn(msg.Connection)
		n.reply(msg, busMessage{Error: code})
	case busAttributes:
		attributes, code := n.attributesLocal(msg.Connection)
		n.reply(msg, busMessage{Attributes: attributes, Error: code})
	case busSetAttribute:
		n.reply(msg, busMessage{Error: n.setAttributeLocal(msg.Connection, msg.Name, msg.Value)})
	case busRemoveAttribute:
		n.reply(msg, busMessage{Error: n.removeAttributeLocal(msg.Connection, msg.Name)})
	case busTags:
		tags, code := n.tagsLocal(msg.Connection)
		n.reply(msg, busMessage{Tags: tags, Error: code})
	case busAddTag:
		n.reply(msg, busMessage{Error: n.addTagLocal(msg.Connection, msg.Tag)})
	case busRemoveTag:
		n.reply(msg, busMessage{Error: n.removeTagLocal(msg.Connection, msg.Tag)})
	}
}
