package com.example.order.api.client;

import com.example.order.api.config.RetryPolicyConfig;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class RetryPolicyTest {

    @SuppressWarnings("unchecked")
    private List<Map<String, ?>> methodConfigs() {
        return (List<Map<String, ?>>) RetryPolicyConfig.serviceConfig().get("methodConfig");
    }

    @Test
    void retriesOnlyTransientCodes() {
        var retryPolicy = (Map<String, ?>) methodConfigs().get(0).get("retryPolicy");
        assertThat((List<String>) retryPolicy.get("retryableStatusCodes"))
                .containsExactlyInAnyOrder("UNAVAILABLE", "RESOURCE_EXHAUSTED");
    }

    @Test
    void deadlineExceededIsNotRetried() {
        var retryPolicy = (Map<String, ?>) methodConfigs().get(0).get("retryPolicy");
        assertThat((List<String>) retryPolicy.get("retryableStatusCodes"))
                .doesNotContain("DEADLINE_EXCEEDED");
    }

    @Test
    void streamingMethodIsExcludedFromRetries() {
        var names = methodConfigs().stream()
                .flatMap(c -> ((List<Map<String, String>>) c.get("name")).stream())
                .map(n -> n.get("method"))
                .toList();
        assertThat(names).doesNotContain("StreamOrderEvents");
    }

    @Test
    void backoffGrowsAndIsBounded() {
        var retryPolicy = (Map<String, ?>) methodConfigs().get(0).get("retryPolicy");
        assertThat((Double) retryPolicy.get("backoffMultiplier")).isGreaterThan(1.0);
        assertThat((String) retryPolicy.get("maxBackoff")).isEqualTo("2s");
        assertThat((Double) retryPolicy.get("maxAttempts")).isEqualTo(4.0);
    }
}
