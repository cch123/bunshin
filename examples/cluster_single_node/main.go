package main

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/xargin/bunshin"
)

type counterService struct {
	value uint64
}

func (s *counterService) OnClusterMessage(_ context.Context, msg bunshin.ClusterMessage) ([]byte, error) {
	if string(msg.Payload) == "increment" {
		s.value++
	}
	response := make([]byte, 8)
	binary.BigEndian.PutUint64(response, s.value)
	return response, nil
}

func main() {
	ctx := context.Background()
	node, err := bunshin.StartClusterNode(ctx, bunshin.ClusterConfig{
		Service: &counterService{},
	})
	if err != nil {
		panic(err)
	}
	defer node.Close(ctx)

	client, err := node.NewClient(ctx)
	if err != nil {
		panic(err)
	}
	egress, err := client.Send(ctx, []byte("increment"))
	if err != nil {
		panic(err)
	}
	fmt.Println(egress.LogPosition, binary.BigEndian.Uint64(egress.Payload))
}
