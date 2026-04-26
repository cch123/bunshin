package bunshin

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	frameMagic              = "BSHN"
	frameVersion      uint8 = 4
	headerLen               = 48
	maxFrameSize            = 64 * 1024
	maxFrameFragments       = int(^uint16(0))
)

var frameByteOrder = binary.LittleEndian

type frameType uint8

const (
	frameData   frameType = 1
	frameAck    frameType = 2
	frameHello  frameType = 3
	frameError  frameType = 4
	frameStatus frameType = 5
	frameNak    frameType = 6
)

type frameFlag uint16

const (
	frameFlagFragment        frameFlag = 1 << 0
	frameFlagResponseChannel frameFlag = 1 << 1
)

type protocolErrorCode uint16

const (
	protocolErrorUnsupportedVersion protocolErrorCode = 1
	protocolErrorUnsupportedType    protocolErrorCode = 2
	protocolErrorMalformedFrame     protocolErrorCode = 3
)

type frame struct {
	typ           frameType
	flags         frameFlag
	streamID      uint32
	sessionID     uint32
	termID        int32
	termOffset    int32
	seq           uint64
	reserved      uint64
	fragmentIndex uint16
	fragmentCount uint16
	payload       []byte
}

type helloPayload struct {
	minVersion uint8
	maxVersion uint8
}

type errorPayload struct {
	code    protocolErrorCode
	message string
}

type statusPayload struct {
	windowLength int
}

type nakPayload struct {
	fromSequence uint64
	toSequence   uint64
}

func encodeResponseChannelPayload(r ResponseChannel) ([]byte, error) {
	transport := []byte(r.Transport)
	remoteAddr := []byte(r.RemoteAddr)
	if len(transport) > int(^uint16(0)) {
		return nil, fmt.Errorf("response channel transport too large: %d bytes", len(transport))
	}
	if len(remoteAddr) > int(^uint16(0)) {
		return nil, fmt.Errorf("response channel remote address too large: %d bytes", len(remoteAddr))
	}
	buf := make([]byte, 8+len(transport)+len(remoteAddr))
	frameByteOrder.PutUint32(buf[0:4], r.StreamID)
	frameByteOrder.PutUint16(buf[4:6], uint16(len(transport)))
	frameByteOrder.PutUint16(buf[6:8], uint16(len(remoteAddr)))
	copy(buf[8:8+len(transport)], transport)
	copy(buf[8+len(transport):], remoteAddr)
	return buf, nil
}

func decodeResponseChannelPayload(payload []byte) (ResponseChannel, error) {
	if len(payload) < 8 {
		return ResponseChannel{}, fmt.Errorf("invalid response channel payload length: %d", len(payload))
	}
	streamID := frameByteOrder.Uint32(payload[0:4])
	transportLen := int(frameByteOrder.Uint16(payload[4:6]))
	remoteAddrLen := int(frameByteOrder.Uint16(payload[6:8]))
	if len(payload) != 8+transportLen+remoteAddrLen {
		return ResponseChannel{}, fmt.Errorf("invalid response channel payload length: %d", len(payload))
	}
	if transportLen == 0 || remoteAddrLen == 0 {
		return ResponseChannel{}, errors.New("invalid empty response channel")
	}
	response := ResponseChannel{
		Transport:  TransportMode(string(payload[8 : 8+transportLen])),
		RemoteAddr: string(payload[8+transportLen:]),
		StreamID:   streamID,
	}
	return normalizeResponseChannel(response, response.Transport)
}

func encodeDataPayload(response ResponseChannel, payload []byte) ([]byte, error) {
	if response.IsZero() {
		return payload, nil
	}
	responsePayload, err := encodeResponseChannelPayload(response)
	if err != nil {
		return nil, err
	}
	if len(responsePayload) > int(^uint16(0)) {
		return nil, fmt.Errorf("response channel payload too large: %d bytes", len(responsePayload))
	}
	buf := make([]byte, 2+len(responsePayload)+len(payload))
	frameByteOrder.PutUint16(buf[0:2], uint16(len(responsePayload)))
	copy(buf[2:2+len(responsePayload)], responsePayload)
	copy(buf[2+len(responsePayload):], payload)
	return buf, nil
}

func decodeDataPayload(f frame) (ResponseChannel, []byte, error) {
	if f.flags&frameFlagResponseChannel == 0 {
		return ResponseChannel{}, f.payload, nil
	}
	if len(f.payload) < 2 {
		return ResponseChannel{}, nil, fmt.Errorf("invalid response data payload length: %d", len(f.payload))
	}
	responsePayloadLen := int(frameByteOrder.Uint16(f.payload[0:2]))
	if len(f.payload) < 2+responsePayloadLen {
		return ResponseChannel{}, nil, fmt.Errorf("invalid response data payload length: %d", len(f.payload))
	}
	response, err := decodeResponseChannelPayload(f.payload[2 : 2+responsePayloadLen])
	if err != nil {
		return ResponseChannel{}, nil, err
	}
	return response, f.payload[2+responsePayloadLen:], nil
}

func encodeFrame(f frame) ([]byte, error) {
	if len(f.payload) > maxFrameSize-headerLen {
		return nil, fmt.Errorf("payload too large: %d bytes", len(f.payload))
	}
	if f.typ == frameData && f.fragmentCount == 0 {
		f.fragmentCount = 1
	}

	buf := make([]byte, headerLen+len(f.payload))
	copy(buf[0:4], frameMagic)
	buf[4] = frameVersion
	buf[5] = byte(f.typ)
	frameByteOrder.PutUint16(buf[6:8], uint16(f.flags))
	frameByteOrder.PutUint32(buf[8:12], f.streamID)
	frameByteOrder.PutUint32(buf[12:16], f.sessionID)
	frameByteOrder.PutUint32(buf[16:20], uint32(f.termID))
	frameByteOrder.PutUint32(buf[20:24], uint32(f.termOffset))
	frameByteOrder.PutUint64(buf[24:32], f.seq)
	frameByteOrder.PutUint32(buf[32:36], uint32(len(f.payload)))
	frameByteOrder.PutUint64(buf[36:44], f.reserved)
	frameByteOrder.PutUint16(buf[44:46], f.fragmentIndex)
	frameByteOrder.PutUint16(buf[46:48], f.fragmentCount)
	copy(buf[headerLen:], f.payload)
	return buf, nil
}

func encodeHelloPayload(h helloPayload) []byte {
	return []byte{h.minVersion, h.maxVersion}
}

func decodeHelloPayload(payload []byte) (helloPayload, error) {
	if len(payload) != 2 {
		return helloPayload{}, fmt.Errorf("invalid hello payload length: %d", len(payload))
	}
	return helloPayload{
		minVersion: payload[0],
		maxVersion: payload[1],
	}, nil
}

func encodeErrorPayload(e errorPayload) []byte {
	msg := []byte(e.message)
	buf := make([]byte, 2+len(msg))
	frameByteOrder.PutUint16(buf[0:2], uint16(e.code))
	copy(buf[2:], msg)
	return buf
}

func decodeErrorPayload(payload []byte) (errorPayload, error) {
	if len(payload) < 2 {
		return errorPayload{}, fmt.Errorf("invalid error payload length: %d", len(payload))
	}
	return errorPayload{
		code:    protocolErrorCode(frameByteOrder.Uint16(payload[0:2])),
		message: string(payload[2:]),
	}, nil
}

func encodeStatusPayload(s statusPayload) []byte {
	buf := make([]byte, 4)
	frameByteOrder.PutUint32(buf[0:4], uint32(s.windowLength))
	return buf
}

func decodeStatusPayload(payload []byte) (statusPayload, error) {
	if len(payload) != 4 {
		return statusPayload{}, fmt.Errorf("invalid status payload length: %d", len(payload))
	}
	windowLength := int(frameByteOrder.Uint32(payload[0:4]))
	if windowLength <= 0 {
		return statusPayload{}, fmt.Errorf("invalid status window length: %d", windowLength)
	}
	return statusPayload{windowLength: windowLength}, nil
}

func encodeNakPayload(n nakPayload) []byte {
	buf := make([]byte, 16)
	frameByteOrder.PutUint64(buf[0:8], n.fromSequence)
	frameByteOrder.PutUint64(buf[8:16], n.toSequence)
	return buf
}

func decodeNakPayload(payload []byte) (nakPayload, error) {
	if len(payload) != 16 {
		return nakPayload{}, fmt.Errorf("invalid nak payload length: %d", len(payload))
	}
	nak := nakPayload{
		fromSequence: frameByteOrder.Uint64(payload[0:8]),
		toSequence:   frameByteOrder.Uint64(payload[8:16]),
	}
	if nak.fromSequence == 0 || nak.toSequence < nak.fromSequence {
		return nakPayload{}, fmt.Errorf("invalid nak sequence range: %d-%d", nak.fromSequence, nak.toSequence)
	}
	return nak, nil
}

func frameHeaderVersion(buf []byte) (uint8, bool) {
	if len(buf) < headerLen || string(buf[0:4]) != frameMagic {
		return 0, false
	}
	return buf[4], true
}

func decodeFrame(buf []byte) (frame, error) {
	return decodeFrameWithPayload(buf, true)
}

func decodeFrameView(buf []byte) (frame, error) {
	return decodeFrameWithPayload(buf, false)
}

func decodeFrameWithPayload(buf []byte, copyPayload bool) (frame, error) {
	if len(buf) < headerLen {
		return frame{}, errors.New("frame too short")
	}
	if string(buf[0:4]) != frameMagic {
		return frame{}, errors.New("invalid frame magic")
	}
	if buf[4] != frameVersion {
		return frame{}, fmt.Errorf("unsupported frame version: %d", buf[4])
	}

	payloadLen := int(frameByteOrder.Uint32(buf[32:36]))
	if payloadLen < 0 || headerLen+payloadLen > len(buf) {
		return frame{}, errors.New("invalid payload length")
	}
	payload := buf[headerLen : headerLen+payloadLen]
	if copyPayload {
		payload = append([]byte(nil), payload...)
	}

	return frame{
		typ:           frameType(buf[5]),
		flags:         frameFlag(frameByteOrder.Uint16(buf[6:8])),
		streamID:      frameByteOrder.Uint32(buf[8:12]),
		sessionID:     frameByteOrder.Uint32(buf[12:16]),
		termID:        int32(frameByteOrder.Uint32(buf[16:20])),
		termOffset:    int32(frameByteOrder.Uint32(buf[20:24])),
		seq:           frameByteOrder.Uint64(buf[24:32]),
		reserved:      frameByteOrder.Uint64(buf[36:44]),
		fragmentIndex: frameByteOrder.Uint16(buf[44:46]),
		fragmentCount: frameByteOrder.Uint16(buf[46:48]),
		payload:       payload,
	}, nil
}

func decodeFrames(buf []byte) ([]frame, error) {
	return decodeFramesWithPayload(buf, true)
}

func decodeFramesView(buf []byte) ([]frame, error) {
	return decodeFramesWithPayload(buf, false)
}

func decodeFramesWithPayload(buf []byte, copyPayload bool) ([]frame, error) {
	if len(buf) == 0 {
		return nil, errors.New("frame too short")
	}

	frames := make([]frame, 0, 1)
	for offset := 0; offset < len(buf); {
		if len(buf)-offset < headerLen {
			return nil, errors.New("frame too short")
		}
		payloadLen := int(frameByteOrder.Uint32(buf[offset+32 : offset+36]))
		frameEnd := offset + headerLen + payloadLen
		if payloadLen < 0 || frameEnd > len(buf) {
			return nil, errors.New("invalid payload length")
		}
		f, err := decodeFrameWithPayload(buf[offset:frameEnd], copyPayload)
		if err != nil {
			return nil, err
		}
		frames = append(frames, f)
		offset = frameEnd
	}
	return frames, nil
}
