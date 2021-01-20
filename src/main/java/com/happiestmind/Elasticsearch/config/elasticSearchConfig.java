package com.happiestmind.Elasticsearch.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.elasticsearch.client.ClientConfiguration;
import org.springframework.data.elasticsearch.client.RestClients;
import org.springframework.stereotype.Component;

@Configuration
@Component
public class elasticSearchConfig {
  @Bean
  public RestHighLevelClient client() {
    ClientConfiguration clientConfiguration =
        ClientConfiguration.builder().connectedTo("localhost:9200").build();
    return RestClients.create(clientConfiguration).rest();
  }

  @Bean
  public RestClient lowLevelClient() {
    return RestClient.builder(new HttpHost("localhost", 9200, "http")).build();
  }
}
