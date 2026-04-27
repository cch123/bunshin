package core

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var (
	ErrPublicationClaimClosed  = errors.New("bunshin publication claim: closed")
	ErrPublicationClaimAborted = errors.New("bunshin publication claim: aborted")
)

type PublicationClaim struct {
	mu      sync.Mutex
	pub     *Publication
	payload []byte
	closed  bool
	aborted bool
}

func (p *Publication) Claim(length int) (*PublicationClaim, error) {
	if p == nil {
		return nil, ErrClosed
	}
	if length < 0 {
		return nil, invalidConfigf("invalid claim length: %d", length)
	}
	if length > p.maxPayload {
		return nil, publicationPayloadTooLargeError(length, p.maxPayload)
	}
	select {
	case <-p.closed:
		return nil, ErrClosed
	default:
	}
	return &PublicationClaim{
		pub:     p,
		payload: make([]byte, length),
	}, nil
}

func (c *PublicationClaim) Buffer() []byte {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.aborted {
		return nil
	}
	return c.payload
}

func (c *PublicationClaim) Commit(ctx context.Context) PublicationOfferResult {
	if c == nil {
		return publicationOfferError(OfferClosed, ErrPublicationClaimClosed)
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return publicationOfferError(OfferClosed, ErrPublicationClaimClosed)
	}
	if c.aborted {
		c.mu.Unlock()
		return publicationOfferError(OfferClosed, ErrPublicationClaimAborted)
	}
	pub := c.pub
	payload := c.payload
	c.pub = nil
	c.payload = nil
	c.closed = true
	c.mu.Unlock()

	return pub.Offer(ctx, payload)
}

func (c *PublicationClaim) Abort() error {
	if c == nil {
		return ErrPublicationClaimClosed
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrPublicationClaimClosed
	}
	if c.aborted {
		return ErrPublicationClaimAborted
	}
	c.pub = nil
	c.payload = nil
	c.aborted = true
	return nil
}

func publicationPayloadTooLargeError(length, maxPayload int) error {
	return fmt.Errorf("%w: publication claim payload too large: %d bytes exceeds max %d", ErrInvalidConfig, length, maxPayload)
}
