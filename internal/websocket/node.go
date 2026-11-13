package websocket

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/chapterjason/frankenphp-websocket/internal/cluster"
	"github.com/gobwas/ws"
)

// connectionsChannel is the channel of the cluster the instances use for each
// other's connections.
const connectionsChannel = "connections"

// Kinds of messages between instances. Pushes go one way; queries and the
// sends to a tag or to all expect an answer.
const (
	busSend               = "send"
	busPing               = "ping"
	busClose              = "close"
	busTag                = "tag"
	busBroadcast          = "broadcast"
	busIsOpen             = "is_open"
	busAttributes         = "attributes"
	busSetAttribute       = "set_attribute"
	busRemoveAttribute    = "remove_attribute"
	busTags               = "tags"
	busAddTag             = "add_tag"
	busRemoveTag          = "remove_tag"
	busConnections        = "connections"
	busConnectionsWithTag = "connections_with_tag"
)

type busMessage struct {
	Kind       string `json:"kind"`
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

// node is one instance: the connections it holds, and its membership in the
// cluster that reaches the other instances.
type node struct {
	sessions *hub
	member   *cluster.Membership
}

// local is the node of this process, the one PHP addresses.
var local = newNode(cluster.Local)

// statusRejoining closes the connections of an instance the cluster reported
// as gone while it was only cut off: 1012, service restart, so clients reconnect.
const statusRejoining ws.StatusCode = 1012

func newNode(member *cluster.Membership) *node {
	n := &node{sessions: newHub(), member: member}
	member.Handle(connectionsChannel, n.receive)
	member.OnReportedGone(func() { n.closeAll(statusRejoining, "rejoining the cluster") })

	return n
}

// closeAll closes every connection held here and returns once their close
// events ran.
func (n *node) closeAll(code ws.StatusCode, reason string) {
	var wg sync.WaitGroup
	for _, s := range n.sessions.all() {
		c := s.attached()
		if c == nil {
			continue
		}
		wg.Go(func() {
			c.close(code, reason)
			<-s.removed
		})
	}
	wg.Wait()
}

// connectionID names a new connection after the instance holding it, so other
// instances can route to it.
func (n *node) connectionID() string {
	return n.member.ID() + "." + rand.Text()
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

	return n.member.Bus() != nil && instance != "" && !n.member.IsOwn(instance)
}

// instance names the instance holding connection: this one for a connection
// held here, otherwise the one its ID names when that is a member known to be
// alive. It asks no one, so a connection that closed on another instance is
// still named after it.
func (n *node) instance(connection string) (string, int64) {
	if _, held := n.sessions.get(connection); held {
		return n.member.ID(), WEBSOCKET_ERROR_NONE
	}

	instance := instanceOf(connection)
	if instance == "" || n.member.IsOwn(instance) || !slices.Contains(n.member.Members(), instance) {
		return "", WEBSOCKET_ERROR_NOT_FOUND
	}

	return instance, WEBSOCKET_ERROR_NONE
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

func openIDs(sessions []*session) []string {
	ids := make([]string, 0)
	for _, s := range sessions {
		if s.openConn() != nil {
			ids = append(ids, s.id)
		}
	}

	return ids
}

func (n *node) openConnections() []string {
	return openIDs(n.sessions.all())
}

func (n *node) openConnectionsWithTag(tag string) []string {
	return openIDs(n.sessions.tagged(tag))
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

// connectionsWithTag lists the open connections with the tag of every instance.
func (n *node) connectionsWithTag(tag string) ([]string, int64) {
	ids := n.openConnectionsWithTag(tag)

	replies, code := n.gather(busMessage{Kind: busConnectionsWithTag, Tag: tag})
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

// errorCode is the WEBSOCKET_ERROR_* code of a cluster error.
func errorCode(err error) int64 {
	switch {
	case err == nil:
		return WEBSOCKET_ERROR_NONE
	case errors.Is(err, cluster.ErrNotJoined):
		return WEBSOCKET_ERROR_NOT_FOUND
	case errors.Is(err, cluster.ErrTimeout):
		return WEBSOCKET_ERROR_TIMEOUT
	default:
		return WEBSOCKET_ERROR_BUS_FAILED
	}
}

func (n *node) publishTo(connection string, msg busMessage) int64 {
	data, err := json.Marshal(msg)
	if err != nil {
		return WEBSOCKET_ERROR_BUS_FAILED
	}

	return errorCode(n.member.Send(instanceOf(connection), connectionsChannel, data))
}

// request asks the instance holding connection and waits for its reply; the
// returned code is the reply's, or WEBSOCKET_ERROR_TIMEOUT.
func (n *node) request(connection string, msg busMessage) (busMessage, int64) {
	data, err := json.Marshal(msg)
	if err != nil {
		return busMessage{}, WEBSOCKET_ERROR_BUS_FAILED
	}

	answer, err := n.member.Request(instanceOf(connection), connectionsChannel, data, 0)
	if err != nil {
		return busMessage{}, errorCode(err)
	}

	var reply busMessage
	if err := json.Unmarshal(answer, &reply); err != nil {
		return busMessage{}, WEBSOCKET_ERROR_BUS_FAILED
	}

	return reply, reply.Error
}

// gather asks every instance and waits for the replies of those known to be
// alive. Missing replies make the code WEBSOCKET_ERROR_TIMEOUT, unless the bus
// reported an error, which then is the code; without a cluster, there is no
// one to ask.
func (n *node) gather(msg busMessage) ([]busMessage, int64) {
	data, err := json.Marshal(msg)
	if err != nil {
		return nil, WEBSOCKET_ERROR_BUS_FAILED
	}

	answers, err := n.member.Gather(connectionsChannel, data)
	if errors.Is(err, cluster.ErrNotJoined) {
		return nil, WEBSOCKET_ERROR_NONE
	}

	replies := make([]busMessage, 0, len(answers))
	for _, answer := range answers {
		var reply busMessage
		if json.Unmarshal(answer.Data, &reply) == nil {
			replies = append(replies, reply)
		}
	}

	return replies, errorCode(err)
}

func answer(respond func([]byte), reply busMessage) {
	data, err := json.Marshal(reply)
	if err == nil {
		respond(data)
	}
}

// receive handles a message of another instance on the websocket channel.
func (n *node) receive(message cluster.Message, respond func([]byte)) {
	var msg busMessage
	if err := json.Unmarshal(message.Data, &msg); err != nil {
		return
	}

	switch msg.Kind {
	case busSend:
		n.sendLocal(msg.Connection, msg.Text, msg.Data)
	case busPing:
		if c, _, _ := n.localConn(msg.Connection); c != nil {
			_ = c.ping(msg.Data)
		}
	case busClose:
		n.closeLocal(msg.Connection, ws.StatusCode(msg.Code), msg.Reason)
	case busTag:
		answer(respond, busMessage{Count: sendToAll(n.sessions.tagged(msg.Tag), msg.Text, msg.Data)})
	case busBroadcast:
		answer(respond, busMessage{Count: sendToAll(n.sessions.all(), msg.Text, msg.Data)})
	case busConnections:
		answer(respond, busMessage{Connections: n.openConnections()})
	case busConnectionsWithTag:
		answer(respond, busMessage{Connections: n.openConnectionsWithTag(msg.Tag)})
	case busIsOpen:
		_, code, _ := n.localConn(msg.Connection)
		answer(respond, busMessage{Error: code})
	case busAttributes:
		attributes, code := n.attributesLocal(msg.Connection)
		answer(respond, busMessage{Attributes: attributes, Error: code})
	case busSetAttribute:
		answer(respond, busMessage{Error: n.setAttributeLocal(msg.Connection, msg.Name, msg.Value)})
	case busRemoveAttribute:
		answer(respond, busMessage{Error: n.removeAttributeLocal(msg.Connection, msg.Name)})
	case busTags:
		tags, code := n.tagsLocal(msg.Connection)
		answer(respond, busMessage{Tags: tags, Error: code})
	case busAddTag:
		answer(respond, busMessage{Error: n.addTagLocal(msg.Connection, msg.Tag)})
	case busRemoveTag:
		answer(respond, busMessage{Error: n.removeTagLocal(msg.Connection, msg.Tag)})
	}
}
