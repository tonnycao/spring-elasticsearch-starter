package io.github.tonnycao.esrest.monitor;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

@Component
public class ElasticsearchMetrics{

    private final MeterRegistry registry;

    public ElasticsearchMetrics(MeterRegistry registry) {
        this.registry = registry;
        // 创建一个计数器，用于记录Elasticsearch的健康状况检查
        Counter.builder("elasticsearch.health")
                .description("Elasticsearch health check result")
                .register(registry);
    }
}
