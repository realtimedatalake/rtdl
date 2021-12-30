package main

import (
	"fmt"

	"github.com/apache/flink-statefun/statefun-sdk-go/v3/pkg/statefun"
)

func ingest(ctx statefun.Context, message statefun.Message) error {
	if !message.Is(statefun.StringType) {
		return fmt.Errorf("unexpected message type %s", message.ValueTypeName())
	}

	var name string
	_ = message.As(statefun.StringType, &name)


	fmt.Println(fmt.Sprintf("Hello %s!", name))

	return nil
}
