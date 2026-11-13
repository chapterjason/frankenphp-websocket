package frankenphpwebsocket

// #cgo linux CFLAGS: -D_GNU_SOURCE
// #include <Zend/zend_types.h>
//
// // Each PHP thread runs on its own OS thread, so this is per PHP thread.
// static __thread long websocket_last_error_code;
//
// static void websocket_set_last_error(long code) { websocket_last_error_code = code; }
// static long websocket_get_last_error(void) { return websocket_last_error_code; }
import "C"
import (
	"errors"
	"maps"
	"slices"
	"unicode/utf8"
	"unsafe"

	"github.com/dunglas/frankenphp"
	"github.com/gobwas/ws"
)

// export_php:const
const WEBSOCKET_AUTO = 0

// export_php:const
const WEBSOCKET_TEXT = 1

// export_php:const
const WEBSOCKET_BINARY = 2

// export_php:const
const WEBSOCKET_ERROR_NONE = 0

// export_php:const
const WEBSOCKET_ERROR_NOT_FOUND = 1

// export_php:const
const WEBSOCKET_ERROR_NOT_OPEN = 2

// export_php:const
const WEBSOCKET_ERROR_INVALID_ARGUMENT = 3

// export_php:const
const WEBSOCKET_ERROR_WRITE_FAILED = 4

// export_php:const
const WEBSOCKET_ERROR_BUS_FAILED = 5

// export_php:const
const WEBSOCKET_ERROR_TIMEOUT = 6

const maxControlPayload = 125

func setLastError(code int64) {
	C.websocket_set_last_error(C.long(code))
}

// succeeded records code as the last error and reports whether it is none.
func succeeded(code int64) bool {
	setLastError(code)

	return code == WEBSOCKET_ERROR_NONE
}

func goString(s *C.zend_string) string {
	return frankenphp.GoString(unsafe.Pointer(s))
}

// messageKind resolves a WEBSOCKET_* message type; text must be valid UTF-8.
func messageKind(data string, messageType int64) (text bool, ok bool) {
	switch messageType {
	case WEBSOCKET_AUTO:
		return utf8.ValidString(data), true
	case WEBSOCKET_TEXT:
		return true, utf8.ValidString(data)
	case WEBSOCKET_BINARY:
		return false, true
	default:
		return false, false
	}
}

func writeError(err error) int64 {
	if errors.Is(err, errConnClosed) {
		return WEBSOCKET_ERROR_NOT_OPEN
	}

	return WEBSOCKET_ERROR_WRITE_FAILED
}

// websocketSend sends a message; type 0 is WEBSOCKET_AUTO, a text message
// when data is valid UTF-8 and a binary message otherwise. A connection held
// by another instance is reached through the bus, and true then means the
// message is on the bus.
//
// export_php:function websocket_send(string $connection, string $data, int $type = 0): bool
func websocketSend(connection *C.zend_string, data *C.zend_string, messageType int64) bool {
	payload := goString(data)
	text, ok := messageKind(payload, messageType)
	if !ok {
		return succeeded(WEBSOCKET_ERROR_INVALID_ARGUMENT)
	}

	return succeeded(local.send(goString(connection), text, []byte(payload)))
}

// websocketSendToTag sends a message to every open connection with the tag,
// on every instance, and returns to how many it was sent; with an instance
// that did not answer in time, the count misses its connections and the last
// error is WEBSOCKET_ERROR_TIMEOUT.
//
// export_php:function websocket_send_to_tag(string $tag, string $data, int $type = 0): int
func websocketSendToTag(tag *C.zend_string, data *C.zend_string, messageType int64) int64 {
	payload := goString(data)
	text, ok := messageKind(payload, messageType)
	if !ok {
		setLastError(WEBSOCKET_ERROR_INVALID_ARGUMENT)

		return 0
	}

	sent, code := local.sendToTag(goString(tag), text, []byte(payload))
	setLastError(code)

	return sent
}

// websocketBroadcast sends a message to every open connection, on every
// instance, and returns to how many it was sent; with an instance that did not
// answer in time, the count misses its connections and the last error is
// WEBSOCKET_ERROR_TIMEOUT.
//
// export_php:function websocket_broadcast(string $data, int $type = 0): int
func websocketBroadcast(data *C.zend_string, messageType int64) int64 {
	payload := goString(data)
	text, ok := messageKind(payload, messageType)
	if !ok {
		setLastError(WEBSOCKET_ERROR_INVALID_ARGUMENT)

		return 0
	}

	sent, code := local.broadcast(text, []byte(payload))
	setLastError(code)

	return sent
}

// websocketPing sends a ping; the payload is at most 125 bytes.
//
// export_php:function websocket_ping(string $connection, string $payload = ""): bool
func websocketPing(connection *C.zend_string, payload *C.zend_string) bool {
	data := goString(payload)
	if len(data) > maxControlPayload {
		return succeeded(WEBSOCKET_ERROR_INVALID_ARGUMENT)
	}

	return succeeded(local.ping(goString(connection), []byte(data)))
}

// websocketClose starts the closing handshake without waiting for the peer to
// answer it. The code is 1000 to 1003, 1007 to 1014 or 3000 to 4999; the
// reason is valid UTF-8 of at most 123 bytes.
//
// export_php:function websocket_close(string $connection, int $code = 1000, string $reason = ""): bool
func websocketClose(connection *C.zend_string, code int64, reason *C.zend_string) bool {
	text := goString(reason)
	if code < 1000 || code > 4999 || len(text) > maxControlPayload-2 ||
		ws.CheckCloseFrameData(ws.StatusCode(code), text) != nil {
		return succeeded(WEBSOCKET_ERROR_INVALID_ARGUMENT)
	}

	return succeeded(local.close(goString(connection), ws.StatusCode(code), text))
}

// websocketIsOpen reports whether messages can be sent to the connection,
// asking the instance holding it.
//
// export_php:function websocket_is_open(string $connection): bool
func websocketIsOpen(connection *C.zend_string) bool {
	return succeeded(local.isOpen(goString(connection)))
}

// websocketConnections lists the IDs of the open connections of every
// instance; with an instance that did not answer in time, the list misses its
// connections and the last error is WEBSOCKET_ERROR_TIMEOUT.
//
// export_php:function websocket_connections(): array
func websocketConnections() unsafe.Pointer {
	ids, code := local.connections()
	setLastError(code)

	return frankenphp.PHPPackedArray(ids)
}

// websocketSetAttribute attaches a value to the connection; events carry it
// under "attributes". It works from the handshake on.
//
// export_php:function websocket_set_attribute(string $connection, string $name, string $value): bool
func websocketSetAttribute(connection *C.zend_string, name *C.zend_string, value *C.zend_string) bool {
	key := goString(name)
	if key == "" {
		return succeeded(WEBSOCKET_ERROR_INVALID_ARGUMENT)
	}

	return succeeded(local.setAttribute(goString(connection), key, goString(value)))
}

// export_php:function websocket_remove_attribute(string $connection, string $name): bool
func websocketRemoveAttribute(connection *C.zend_string, name *C.zend_string) bool {
	return succeeded(local.removeAttribute(goString(connection), goString(name)))
}

// websocketAttributes returns the attributes of the connection, an empty array
// with the last error set when they cannot be had.
//
// export_php:function websocket_attributes(string $connection): array
func websocketAttributes(connection *C.zend_string) unsafe.Pointer {
	attributes, code := local.attributes(goString(connection))
	setLastError(code)

	names := slices.Sorted(maps.Keys(attributes))
	array := frankenphp.AssociativeArray[string]{Map: attributes, Order: names}
	if array.Map == nil {
		array.Map = map[string]string{}
	}

	return frankenphp.PHPAssociativeArray(array)
}

// websocketTag adds the connection to a tag that websocket_send_to_tag()
// addresses. It works from the handshake on.
//
// export_php:function websocket_tag(string $connection, string $tag): bool
func websocketTag(connection *C.zend_string, tag *C.zend_string) bool {
	name := goString(tag)
	if name == "" {
		return succeeded(WEBSOCKET_ERROR_INVALID_ARGUMENT)
	}

	return succeeded(local.addTag(goString(connection), name))
}

// export_php:function websocket_untag(string $connection, string $tag): bool
func websocketUntag(connection *C.zend_string, tag *C.zend_string) bool {
	return succeeded(local.removeTag(goString(connection), goString(tag)))
}

// websocketTags returns the tags of the connection, an empty array with the
// last error set when they cannot be had.
//
// export_php:function websocket_tags(string $connection): array
func websocketTags(connection *C.zend_string) unsafe.Pointer {
	tags, code := local.tags(goString(connection))
	setLastError(code)

	if tags == nil {
		tags = []string{}
	}

	return frankenphp.PHPPackedArray(tags)
}

// websocketLastError returns the WEBSOCKET_ERROR_* code of the last
// websocket_* call made by this PHP thread.
//
// export_php:function websocket_last_error(): int
func websocketLastError() int64 {
	return int64(C.websocket_get_last_error())
}
