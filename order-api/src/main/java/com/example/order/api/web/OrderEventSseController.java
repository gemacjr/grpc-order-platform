package com.example.order.api.web;

import com.example.order.api.client.ResilientOrderClient;
import com.example.order.api.dto.DtoMapper;
import com.example.order.api.dto.OrderEventDto;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;

@RestController
@RequestMapping("/api/v1/orders/events")
public class OrderEventSseController {

    private final ResilientOrderClient client;

    public OrderEventSseController(ResilientOrderClient client) {
        this.client = client;
    }

    /**
     * The gRPC server stream maps one-to-one onto SSE. Backpressure propagates:
     * a slow HTTP consumer slows the Flux, which slows the gRPC request-n signal,
     * which stops the server writing. Nothing buffers unboundedly.
     */
    @GetMapping(produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<OrderEventDto> stream(@RequestParam(required = false) String customerId,
                                      @RequestParam(defaultValue = "0") long fromSequence) {
        return client.streamEvents(customerId, fromSequence).map(DtoMapper::toDto);
    }
}
