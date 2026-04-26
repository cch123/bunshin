package bunshin

import (
	"context"
	"net"
)

// ExclusivePublication is a single-writer publication facade. Callers must not
// use one ExclusivePublication concurrently from multiple goroutines.
type ExclusivePublication struct {
	pub *Publication
}

func DialExclusivePublication(cfg PublicationConfig) (*ExclusivePublication, error) {
	pub, err := DialPublication(cfg)
	if err != nil {
		return nil, err
	}
	return &ExclusivePublication{pub: pub}, nil
}

func (p *Publication) Exclusive() *ExclusivePublication {
	if p == nil {
		return nil
	}
	return &ExclusivePublication{pub: p}
}

func (p *ExclusivePublication) Send(ctx context.Context, payload []byte) error {
	if p == nil || p.pub == nil {
		return ErrClosed
	}
	return p.pub.Send(ctx, payload)
}

func (p *ExclusivePublication) Offer(ctx context.Context, payload []byte) PublicationOfferResult {
	if p == nil || p.pub == nil {
		return publicationOfferError(OfferClosed, ErrClosed)
	}
	return p.pub.Offer(ctx, payload)
}

func (p *ExclusivePublication) OfferVectored(ctx context.Context, payloads ...[]byte) PublicationOfferResult {
	if p == nil || p.pub == nil {
		return publicationOfferError(OfferClosed, ErrClosed)
	}
	return p.pub.OfferVectored(ctx, payloads...)
}

func (p *ExclusivePublication) SendVectored(ctx context.Context, payloads ...[]byte) error {
	if p == nil || p.pub == nil {
		return ErrClosed
	}
	return p.pub.SendVectored(ctx, payloads...)
}

func (p *ExclusivePublication) Claim(length int) (*PublicationClaim, error) {
	if p == nil || p.pub == nil {
		return nil, ErrClosed
	}
	return p.pub.Claim(length)
}

func (p *ExclusivePublication) AddDestination(remoteAddr string) error {
	if p == nil || p.pub == nil {
		return ErrClosed
	}
	return p.pub.AddDestination(remoteAddr)
}

func (p *ExclusivePublication) RemoveDestination(remoteAddr string) error {
	if p == nil || p.pub == nil {
		return ErrClosed
	}
	return p.pub.RemoveDestination(remoteAddr)
}

func (p *ExclusivePublication) Destinations() []string {
	if p == nil || p.pub == nil {
		return nil
	}
	return p.pub.Destinations()
}

func (p *ExclusivePublication) DestinationEndpoints() []string {
	if p == nil || p.pub == nil {
		return nil
	}
	return p.pub.DestinationEndpoints()
}

func (p *ExclusivePublication) ReResolveDestinations() error {
	if p == nil || p.pub == nil {
		return ErrClosed
	}
	return p.pub.ReResolveDestinations()
}

func (p *ExclusivePublication) LocalAddr() net.Addr {
	if p == nil || p.pub == nil {
		return nil
	}
	return p.pub.LocalAddr()
}

func (p *ExclusivePublication) ChannelURI() ChannelURI {
	if p == nil || p.pub == nil {
		return ChannelURI{}
	}
	return p.pub.ChannelURI()
}

func (p *ExclusivePublication) Close() error {
	if p == nil || p.pub == nil {
		return nil
	}
	return p.pub.Close()
}
