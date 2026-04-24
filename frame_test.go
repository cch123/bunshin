package bunshin

import "testing"

func TestFrameRoundTrip(t *testing.T) {
	in := frame{
		typ:       frameData,
		streamID:  7,
		sessionID: 11,
		seq:       42,
		payload:   []byte("hello"),
	}
	buf, err := encodeFrame(in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := decodeFrame(buf)
	if err != nil {
		t.Fatal(err)
	}
	if out.typ != in.typ || out.streamID != in.streamID || out.sessionID != in.sessionID || out.seq != in.seq {
		t.Fatalf("decoded header mismatch: %#v", out)
	}
	if string(out.payload) != string(in.payload) {
		t.Fatalf("decoded payload = %q, want %q", out.payload, in.payload)
	}
}

func TestFrameRejectsInvalidChecksum(t *testing.T) {
	buf, err := encodeFrame(frame{
		typ:     frameData,
		payload: []byte("hello"),
	})
	if err != nil {
		t.Fatal(err)
	}
	buf[len(buf)-1] ^= 0xff
	if _, err := decodeFrame(buf); err == nil {
		t.Fatal("expected checksum error")
	}
}

func TestHelloPayloadRoundTrip(t *testing.T) {
	in := helloPayload{minVersion: 1, maxVersion: 3}
	out, err := decodeHelloPayload(encodeHelloPayload(in))
	if err != nil {
		t.Fatal(err)
	}
	if out != in {
		t.Fatalf("decoded hello = %#v, want %#v", out, in)
	}
}

func TestErrorPayloadRoundTrip(t *testing.T) {
	in := errorPayload{
		code:    protocolErrorUnsupportedVersion,
		message: "unsupported protocol version",
	}
	out, err := decodeErrorPayload(encodeErrorPayload(in))
	if err != nil {
		t.Fatal(err)
	}
	if out != in {
		t.Fatalf("decoded error = %#v, want %#v", out, in)
	}
}

func FuzzDecodeFrame(f *testing.F) {
	valid, err := encodeFrame(frame{
		typ:       frameData,
		streamID:  1,
		sessionID: 2,
		seq:       3,
		payload:   []byte("seed"),
	})
	if err != nil {
		f.Fatal(err)
	}

	f.Add(valid)
	f.Add([]byte{})
	f.Add([]byte("not-a-frame"))
	f.Add(valid[:headerLen])

	f.Fuzz(func(t *testing.T, buf []byte) {
		_, _ = decodeFrame(buf)
	})
}
