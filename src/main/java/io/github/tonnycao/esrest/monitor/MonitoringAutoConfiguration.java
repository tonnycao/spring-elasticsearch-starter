package io.github.tonnycao.esrest.monitor;

import org.springframework.boot.actuate.autoconfigure.metrics.MetricsAutoConfiguration;
import org.springframework.boot.actuate.autoconfigure.metrics.export.simple.SimpleMetricsExportAutoConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;

@EnableAutoConfiguration(exclude = {
        MetricsAutoConfiguration.class,
        SimpleMetricsExportAutoConfiguration.class
})
public class MonitoringAutoConfiguration {
}
