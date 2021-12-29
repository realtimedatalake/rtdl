package main

import (
	"fmt"

	"github.com/apache/flink-statefun/statefun-sdk-go/v3/pkg/statefun"
)

func (sf *Ingest) Invoke(ctx statefun.Context, message statefun.Message) error {
	if !message.Is(statefun.StringType) {
		return fmt.Errorf("unexpected message type %s", message.ValueTypeName())
	}

	var name string
	_ = message.As(statefun.StringType, &name)

	//storage := ctx.Storage()

	ctx.Send(statefun.MessageBuilder{
		Target: statefun.Address{
			FunctionType: statefun.TypeNameFrom("com.rtdl.ingest.sf/ingest"),
			Id:           name,
		},
		Value: fmt.Sprintf("Hello %s!", name),
	})

	return nil
}
