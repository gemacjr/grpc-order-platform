package com.example.order.service.domain;

public record OrderLine(String sku, int quantity, long unitPriceMinor) {
    public long lineTotalMinor() {
        return (long) quantity * unitPriceMinor;
    }
}
