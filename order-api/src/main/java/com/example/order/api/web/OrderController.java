package com.example.order.api.web;

import com.example.order.api.client.ResilientOrderClient;
import com.example.order.api.dto.*;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/orders")
public class OrderController {

    private final ResilientOrderClient client;

    public OrderController(ResilientOrderClient client) {
        this.client = client;
    }

    @PostMapping
    public Mono<ResponseEntity<OrderResponseDto>> create(@Valid @RequestBody CreateOrderRequestDto request) {
        return client.create(request.customerId(), DtoMapper.toProtoLines(request.lines()),
                        request.currency(), request.idempotencyKey())
                .map(response -> {
                    var dto = DtoMapper.toDto(response.getOrder());
                    // A replay is not a creation: 200 tells the caller nothing new happened.
                    return response.getIdempotentReplay()
                            ? ResponseEntity.ok(dto)
                            : ResponseEntity.created(URI.create("/api/v1/orders/" + dto.id())).body(dto);
                });
    }

    @GetMapping("/{id}")
    public Mono<OrderResponseDto> get(@PathVariable UUID id) {
        return client.get(id).map(DtoMapper::toDto);
    }

    @GetMapping
    public Mono<OrderPageDto> list(@RequestParam(required = false) String customerId,
                                   @RequestParam(required = false) String status,
                                   @RequestParam(defaultValue = "50") int pageSize,
                                   @RequestParam(required = false) String pageToken) {
        return client.list(customerId, status == null ? null : DtoMapper.fromShortName(status),
                        pageSize, pageToken)
                .map(response -> new OrderPageDto(
                        response.getOrdersList().stream().map(DtoMapper::toDto).toList(),
                        response.getNextPageToken()));
    }

    @PatchMapping("/{id}/status")
    public Mono<OrderResponseDto> updateStatus(@PathVariable UUID id,
                                               @Valid @RequestBody UpdateStatusRequestDto request) {
        return client.updateStatus(id, DtoMapper.fromShortName(request.status()), request.expectedVersion())
                .map(DtoMapper::toDto);
    }
}
