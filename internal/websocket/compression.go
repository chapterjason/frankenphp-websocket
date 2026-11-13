package websocket

import (
	"bytes"
	"compress/flate"
	"errors"
	"io"
	"strconv"
	"strings"
)

// permessage-deflate, RFC 7692.
const deflateExtension = "permessage-deflate"

// deflateTail ends every flushed deflate block; RFC 7692 strips it on the wire.
var deflateTail = []byte{0x00, 0x00, 0xff, 0xff}

var errMessageTooBig = errors.New("websocket message too big")

// negotiateDeflate picks the first acceptable permessage-deflate offer of the
// client and returns the response value for it. Context takeover is disabled
// both ways, so every message is compressed on its own.
func negotiateDeflate(offers []string) (string, bool) {
	for _, header := range offers {
		for _, offer := range strings.Split(header, ",") {
			params := strings.Split(offer, ";")
			if !strings.EqualFold(strings.TrimSpace(params[0]), deflateExtension) {
				continue
			}

			if response, ok := deflateResponse(params[1:]); ok {
				return response, true
			}
		}
	}

	return "", false
}

func deflateResponse(params []string) (string, bool) {
	response := deflateExtension + "; server_no_context_takeover; client_no_context_takeover"
	seen := make(map[string]bool, len(params))

	for _, param := range params {
		name, value, hasValue := strings.Cut(strings.TrimSpace(param), "=")
		name = strings.ToLower(strings.TrimSpace(name))
		value = strings.Trim(strings.TrimSpace(value), `"`)

		if seen[name] {
			return "", false
		}
		seen[name] = true

		switch name {
		case "server_no_context_takeover", "client_no_context_takeover":
			if hasValue {
				return "", false
			}
		case "server_max_window_bits":
			// compress/flate always uses a 32 KiB window, which is 15 bits.
			if value != "15" {
				return "", false
			}
			response += "; server_max_window_bits=15"
		case "client_max_window_bits":
			if hasValue {
				bits, err := strconv.Atoi(value)
				if err != nil || bits < 8 || bits > 15 {
					return "", false
				}
			}
		default:
			return "", false
		}
	}

	return response, true
}

// wantsDeflate reports whether PHP's handshake response asks for compression.
func wantsDeflate(values []string) bool {
	for _, header := range values {
		for _, extension := range strings.Split(header, ",") {
			name, _, _ := strings.Cut(extension, ";")
			if strings.EqualFold(strings.TrimSpace(name), deflateExtension) {
				return true
			}
		}
	}

	return false
}

func deflate(data []byte) ([]byte, error) {
	var buf bytes.Buffer

	w, err := flate.NewWriter(&buf, flate.DefaultCompression)
	if err != nil {
		return nil, err
	}
	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Flush(); err != nil {
		return nil, err
	}

	return bytes.TrimSuffix(buf.Bytes(), deflateTail), nil
}

func inflate(data []byte, limit int64) ([]byte, error) {
	r := flate.NewReader(io.MultiReader(bytes.NewReader(data), bytes.NewReader(deflateTail)))
	defer func() { _ = r.Close() }()

	out, err := io.ReadAll(io.LimitReader(r, limit+1))
	// The restored tail ends a flushed block, not the deflate stream, so the
	// reader reports the end of the input as unexpected.
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, err
	}

	if int64(len(out)) > limit {
		return nil, errMessageTooBig
	}

	return out, nil
}
