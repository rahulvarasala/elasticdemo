package com.example.elastic;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetAddress;

@Configuration
public class BinLogConfig {

    @Bean
    public BinaryLogClient getBinLogclient() throws Exception {

//        Settings esSettings = Settings.settingsBuilder()
//                .put("cluster.name", EsClusterName)
//                .build();

        //https://www.elastic.co/guide/en/elasticsearch/guide/current/_transport_client_versus_node_client.html
        /*return TransportClient.builder()
                .settings(esSettings)
                .build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(EsHost), EsPort));*/
        BinaryLogClient binLogClient =
                new BinaryLogClient("localhost", 3306, "root", "nandu1234");

        System.out.println("client created");
        binLogClient.registerEventListener(event -> {
            System.out.println(event);
        });
        binLogClient.connect();
        System.out.println("Binlog client started");

        return binLogClient;

// on shutdown

        //client.close();


    }
}
