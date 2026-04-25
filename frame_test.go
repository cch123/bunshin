package bunshin

import "testing"

func TestFrameRoundTrip(t *testing.T) {
	in := frame{
		typ:        frameData,
		streamID:   7,
		sessionID:  11,
		termID:     13,
		termOffset: 17,
		seq:        42,
		reserved:   99,
		payload:    []byte("hello"),
	}
	buf, err := encodeFrame(in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := decodeFrame(buf)
	if err != nil {
		t.Fatal(err)
	}
	if out.typ != in.typ || out.streamID != in.streamID || out.sessionID != in.sessionID ||
		out.termID != in.termID || out.termOffset != in.termOffset || out.seq != in.seq || out.reserved != in.reserved {
		t.Fatalf("decoded header mismatch: %#v", out)
	}
	if string(out.payload) != string(in.payload) {
		t.Fatalf("decoded payload = %q, want %q", out.payload, in.payload)
	}
}

func TestFrameWireEncoding(t *testing.T) {
	buf, err := encodeFrame(frame{
		typ:        frameData,
		streamID:   0x01020304,
		sessionID:  0x05060708,
		termID:     0x090a0b0c,
		termOffset: 0x0d0e0f10,
		seq:        0x1112131415161718,
		reserved:   0x191a1b1c1d1e1f20,
		payload:    []byte("x"),
	})
	if err != nil {
		t.Fatal(err)
	}

	if string(buf[0:4]) != frameMagic {
		t.Fatalf("magic bytes = %q, want %q", buf[0:4], frameMagic)
	}
	if buf[4] != frameVersion {
		t.Fatalf("version byte = %d, want %d", buf[4], frameVersion)
	}
	if got, want := buf[8:12], []byte{0x04, 0x03, 0x02, 0x01}; string(got) != string(want) {
		t.Fatalf("stream id bytes = %x, want %x", got, want)
	}
	if got, want := buf[16:20], []byte{0x0c, 0x0b, 0x0a, 0x09}; string(got) != string(want) {
		t.Fatalf("term id bytes = %x, want %x", got, want)
	}
	if got, want := buf[20:24], []byte{0x10, 0x0f, 0x0e, 0x0d}; string(got) != string(want) {
		t.Fatalf("term offset bytes = %x, want %x", got, want)
	}
	if got, want := buf[36:44], []byte{0x20, 0x1f, 0x1e, 0x1d, 0x1c, 0x1b, 0x1a, 0x19}; string(got) != string(want) {
		t.Fatalf("reserved value bytes = %x, want %x", got, want)
	}
}

func TestFrameAllowsPayloadMutationWithoutReservedValueValidation(t *testing.T) {
	buf, err := encodeFrame(frame{
		typ:     frameData,
		payload: []byte("hello"),
	})
	if err != nil {
		t.Fatal(err)
	}
	buf[len(buf)-1] ^= 0xff
	if _, err := decodeFrame(buf); err != nil {
		t.Fatal(err)
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
