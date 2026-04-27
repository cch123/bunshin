package core

import "context"

type PublicationOfferStatus int

const (
	OfferAccepted PublicationOfferStatus = iota
	OfferBackPressured
	OfferClosed
	OfferPayloadTooLarge
	OfferFailed
)

type PublicationOfferResult struct {
	Status   PublicationOfferStatus
	Position int64
	Err      error
}

func (r PublicationOfferResult) Accepted() bool {
	return r.Status == OfferAccepted
}

func (p *Publication) Offer(ctx context.Context, payload []byte) PublicationOfferResult {
	if p != nil && p.transportMode == TransportUDP {
		return p.offerUDP(ctx, payload, false)
	}
	return p.offerQUIC(ctx, payload, false)
}

func (p *Publication) OfferVectored(ctx context.Context, payloads ...[]byte) PublicationOfferResult {
	return p.Offer(ctx, gatherPayloads(payloads))
}

func (p *Publication) SendVectored(ctx context.Context, payloads ...[]byte) error {
	return p.Send(ctx, gatherPayloads(payloads))
}

func publicationOfferError(status PublicationOfferStatus, err error) PublicationOfferResult {
	return PublicationOfferResult{Status: status, Err: err}
}

func gatherPayloads(payloads [][]byte) []byte {
	switch len(payloads) {
	case 0:
		return nil
	case 1:
		return payloads[0]
	}
	total := 0
	for _, payload := range payloads {
		total += len(payload)
	}
	combined := make([]byte, 0, total)
	for _, payload := range payloads {
		combined = append(combined, payload...)
	}
	return combined
}
