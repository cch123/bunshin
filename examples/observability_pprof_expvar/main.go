package main

import (
	"context"
	"expvar"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"time"

	"github.com/xargin/bunshin"
)

func main() {
	metrics := &bunshin.Metrics{}
	expvar.Publish("bunshin", expvar.Func(func() any {
		return metrics.Snapshot()
	}))

	sub, err := bunshin.ListenSubscription(bunshin.SubscriptionConfig{
		StreamID:  1,
		LocalAddr: "127.0.0.1:0",
		Metrics:   metrics,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = sub.Serve(ctx, func(_ context.Context, msg bunshin.Message) error {
			fmt.Printf("%s\n", msg.Payload)
			return nil
		})
	}()

	pub, err := bunshin.DialPublication(bunshin.PublicationConfig{
		StreamID:   1,
		RemoteAddr: sub.LocalAddr().String(),
		Metrics:    metrics,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer pub.Close()

	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				_ = pub.Send(ctx, []byte(now.Format(time.RFC3339Nano)))
			}
		}
	}()

	log.Println("serving pprof and expvar on http://127.0.0.1:6060/debug/vars")
	log.Fatal(http.ListenAndServe("127.0.0.1:6060", nil))
}
