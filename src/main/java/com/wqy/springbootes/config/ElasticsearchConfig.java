package com.wqy.springbootes.config;

import com.wqy.springbootes.entity.House;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetAddress;
import java.net.UnknownHostException;

@Configuration
public class ElasticsearchConfig  {

    @Value("${elasticsearch.host}")
    private String esHost;

    @Value("${elasticsearch.port}")
    private int esPort;

    @Value("${elasticsearch.cluster.name}")
    private String esName;



    @Bean
    public TransportClient esClient() throws UnknownHostException {
        Settings settings = Settings.builder()
                //.put("cluster.name","elasticsearch")
                .put("cluster.name", this.esName)
                //.put("client.transport.sniff",true)
                .build();
        /*InetSocketTransportAddress master = new InetSocketTransportAddress(
                InetAddress.getByName("192.168.137.101"),9300
        );*/

        InetSocketTransportAddress master = new InetSocketTransportAddress(
                InetAddress.getByName(esHost), esPort);
        TransportClient client = TransportClient.builder().settings(settings).build().addTransportAddress(master);

        return client;
    }

}
