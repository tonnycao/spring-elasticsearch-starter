package io.github.tonnycao.esrest.config;


import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "elasticsearch.connection")
public class ElasticsearchProperties {

    @Value("elasticsearch.connection.hosts")
    private String hosts;

    @Value("elasticsearch.connection.username")
    private String username;

    @Value("elasticsearch.connection.password")
    private String password;

    @Value("elasticsearch.connection.timeout")
    private String timeout;

    @Value("elasticsearch.connection.scheme")
    private String scheme;

    @Value("elasticsearch.connection.port")
    private String port;

    @Value("elasticsearch.connection.socket.timeout")
    private String socketTimeout;

}
