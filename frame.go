package bunshin

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	frameMagic         = "BSHN"
	frameVersion uint8 = 3
	headerLen          = 44
	maxFrameSize       = 64 * 1024
)

var frameByteOrder = binary.LittleEndian

type frameType uint8

const (
	frameData  frameType = 1
	frameAck   frameType = 2
	frameHello frameType = 3
	frameError frameType = 4
)

type protocolErrorCode uint16

const (
	protocolErrorUnsupportedVersion protocolErrorCode = 1
	protocolErrorUnsupportedType    protocolErrorCode = 2
	protocolErrorMalformedFrame     protocolErrorCode = 3
)

type frame struct {
	typ        frameType
	streamID   uint32
	sessionID  uint32
	termID     int32
	termOffset int32
	seq        uint64
	reserved   uint64
	payload    []byte
}

type helloPayload struct {
	minVersion uint8
	maxVersion uint8
}

type errorPayload struct {
	code    protocolErrorCode
	message string
}

func encodeFrame(f frame) ([]byte, error) {
	if len(f.payload) > maxFrameSize-headerLen {
		return nil, fmt.Errorf("payload too large: %d bytes", len(f.payload))
	}

	buf := make([]byte, headerLen+len(f.payload))
	copy(buf[0:4], frameMagic)
	buf[4] = frameVersion
	buf[5] = byte(f.typ)
	frameByteOrder.PutUint16(buf[6:8], 0)
	frameByteOrder.PutUint32(buf[8:12], f.streamID)
	frameByteOrder.PutUint32(buf[12:16], f.sessionID)
	frameByteOrder.PutUint32(buf[16:20], uint32(f.termID))
	frameByteOrder.PutUint32(buf[20:24], uint32(f.termOffset))
	frameByteOrder.PutUint64(buf[24:32], f.seq)
	frameByteOrder.PutUint32(buf[32:36], uint32(len(f.payload)))
	frameByteOrder.PutUint64(buf[36:44], f.reserved)
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

func frameHeaderVersion(buf []byte) (uint8, bool) {
	if len(buf) < headerLen || string(buf[0:4]) != frameMagic {
		return 0, false
	}
	return buf[4], true
}

func decodeFrame(buf []byte) (frame, error) {
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

	return frame{
		typ:        frameType(buf[5]),
		streamID:   frameByteOrder.Uint32(buf[8:12]),
		sessionID:  frameByteOrder.Uint32(buf[12:16]),
		termID:     int32(frameByteOrder.Uint32(buf[16:20])),
		termOffset: int32(frameByteOrder.Uint32(buf[20:24])),
		seq:        frameByteOrder.Uint64(buf[24:32]),
		reserved:   frameByteOrder.Uint64(buf[36:44]),
		payload:    append([]byte(nil), payload...),
	}, nil
}
