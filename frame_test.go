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
