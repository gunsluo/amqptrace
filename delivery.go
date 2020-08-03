package amqptrace

import (
	"context"

	"github.com/streadway/amqp"
	"go.opentelemetry.io/otel/api/correlation"
	"go.opentelemetry.io/otel/api/kv"
	"go.opentelemetry.io/otel/api/trace"
)

// DeliveryHandle is trace handler of the delivery
type DeliveryHandle struct {
	tracer trace.Tracer
	opts   *deliveryHandleOptions
}

// DeliveryHandleOption is optional configuration of delivery handle
type DeliveryHandleOption func(*deliveryHandleOptions)

func DeliveryHandleComopenentName(componentName string) DeliveryHandleOption {
	return func(o *deliveryHandleOptions) {
		o.componentName = componentName
	}
}

type deliveryHandleOptions struct {
	componentName string
}

func applyDeliveryHandleOptions(opts ...DeliveryHandleOption) *deliveryHandleOptions {
	o := &deliveryHandleOptions{componentName: "net/amqp"}
	for _, opt := range opts {
		opt(o)
	}

	return o
}

// NewDeliveryHandle create a DeliveryHandle
func NewDeliveryHandle(tracer trace.Tracer, opts ...DeliveryHandleOption) *DeliveryHandle {
	o := applyDeliveryHandleOptions(opts...)
	h := &DeliveryHandle{tracer: tracer, opts: o}

	return h
}

func (h *DeliveryHandle) Do(msg amqp.Delivery, handle func(context.Context, amqp.Delivery) error) {
	ctx := context.Background()
	entries, spCtx := Extract(ctx, msg.Headers)
	ctx = correlation.ContextWithMap(ctx, correlation.NewMap(correlation.MapUpdate{
		MultiKV: entries,
	}))

	ctx, span := h.tracer.Start(
		trace.ContextWithRemoteSpanContext(ctx, spCtx),
		msg.Exchange+"."+msg.RoutingKey,
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes(
			kv.String("exchange", msg.Exchange),
			kv.String("routekey", msg.RoutingKey),
			kv.String("content-type", msg.ContentType),
			kv.Key("component").String(h.opts.componentName),
		),
	)
	defer span.End()

	if err := handle(ctx, msg); err != nil {
		span.SetAttribute("error", err)
	}
}
