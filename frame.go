package bunshin

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	frameMagic   uint32 = 0x4253484e // BSHN
	frameVersion uint8  = 1
	headerLen           = 28
	maxFrameSize        = 64 * 1024
)

type frameType uint8

const (
	frameData frameType = 1
	frameAck  frameType = 2
)

type frame struct {
	typ       frameType
	streamID  uint32
	sessionID uint32
	seq       uint64
	payload   []byte
}

func encodeFrame(f frame) ([]byte, error) {
	if len(f.payload) > maxFrameSize-headerLen {
		return nil, fmt.Errorf("payload too large: %d bytes", len(f.payload))
	}

	buf := make([]byte, headerLen+len(f.payload))
	binary.BigEndian.PutUint32(buf[0:4], frameMagic)
	buf[4] = frameVersion
	buf[5] = byte(f.typ)
	binary.BigEndian.PutUint16(buf[6:8], 0)
	binary.BigEndian.PutUint32(buf[8:12], f.streamID)
	binary.BigEndian.PutUint32(buf[12:16], f.sessionID)
	binary.BigEndian.PutUint64(buf[16:24], f.seq)
	binary.BigEndian.PutUint32(buf[24:28], uint32(len(f.payload)))
	copy(buf[headerLen:], f.payload)
	return buf, nil
}

func decodeFrame(buf []byte) (frame, error) {
	if len(buf) < headerLen {
		return frame{}, errors.New("frame too short")
	}
	if binary.BigEndian.Uint32(buf[0:4]) != frameMagic {
		return frame{}, errors.New("invalid frame magic")
	}
	if buf[4] != frameVersion {
		return frame{}, fmt.Errorf("unsupported frame version: %d", buf[4])
	}

	payloadLen := int(binary.BigEndian.Uint32(buf[24:28]))
	if payloadLen < 0 || headerLen+payloadLen > len(buf) {
		return frame{}, errors.New("invalid payload length")
	}

	return frame{
		typ:       frameType(buf[5]),
		streamID:  binary.BigEndian.Uint32(buf[8:12]),
		sessionID: binary.BigEndian.Uint32(buf[12:16]),
		seq:       binary.BigEndian.Uint64(buf[16:24]),
		payload:   append([]byte(nil), buf[headerLen:headerLen+payloadLen]...),
	}, nil
}
