package io.github.tonnycao.esrest.config;

import cn.hutool.core.util.StrUtil;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

@Configuration
@ConditionalOnClass(RestHighLevelClient.class)
@EnableConfigurationProperties(ElasticsearchProperties.class)
public class ElasticsearchConfig {

    @Autowired
    private ElasticsearchProperties properties;

    @Bean
    public RestHighLevelClient createClient() {

        RestHighLevelClient client = null;

        if(StrUtil.isNotBlank(properties.getUsername()) && StrUtil.isNotBlank(properties.getPassword())){

            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(properties.getUsername(), properties.getPassword()));
            client = new RestHighLevelClient(
                    RestClient.builder(getHttpHosts(properties.getHosts(), Integer.valueOf(properties.getPort())))
                            .setHttpClientConfigCallback((HttpAsyncClientBuilder httpAsyncClientBuilder) -> {
                                httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                                // httpclient保活策略
                                httpAsyncClientBuilder.setKeepAliveStrategy(((response, context) -> Duration.ofMinutes(5).toMillis()));
                                return httpAsyncClientBuilder;
                            })
            );
        }else{
            client = new RestHighLevelClient(RestClient.builder(getHttpHosts(properties.getHosts(), Integer.valueOf(properties.getPort())))
                    .setHttpClientConfigCallback((HttpAsyncClientBuilder httpAsyncClientBuilder) -> {
                        // httpclient保活策略
                        httpAsyncClientBuilder.setKeepAliveStrategy(((response, context) -> Duration.ofMinutes(5).toMillis()));
                        return httpAsyncClientBuilder;
                    })
            );

        }
        return client;
    }

    /**
     * 创建 HttpHost 对象
     *
     * @return 返回 HttpHost 对象数组
     */
    private HttpHost[] createHttpHost() {
        List<String> ipAddressList = new ArrayList<>();
        ipAddressList.add(properties.getHosts()+":"+properties.getPort());

        HttpHost[] httpHosts = new HttpHost[ipAddressList.size()];
        for (int i = 0, len = ipAddressList.size(); i < len; i++) {
            String ipAddress = ipAddressList.get(i);
            String[] values = ipAddress.split(":");

            String ip = values[0];
            int port = Integer.parseInt(values[1]);
            // 创建 HttpHost
            httpHosts[i] = new HttpHost(ip, port, properties.getScheme());
        }
        return httpHosts;
    }

    private HttpHost[] getHttpHosts(String clientIps, int esHttpPort) {
        String[] clientIpList = clientIps.split(",");
        HttpHost[] httpHosts = new HttpHost[clientIpList.length];
        for (int i = 0; i < clientIpList.length; i++) {
            httpHosts[i] = new HttpHost(clientIpList[i], esHttpPort, properties.getScheme());
        }
        return httpHosts;
    }

}
