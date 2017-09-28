package com.xxx.demons.kafkamessage.util;

import com.google.gson.Gson;

import org.apache.curator.framework.CuratorFramework;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Scope;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import io.vertx.core.Vertx;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.producer.KafkaProducer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
//@Component
@Scope(BeanDefinition.SCOPE_SINGLETON)
public class KafkaFactory {
    private final Gson GSON = new Gson();
    Map<String, String> config;
    @Resource
    private CuratorFramework zookeeper;
    private Properties props;

    @Resource
    private Vertx vertx;

    @PostConstruct
    private void initialize() throws Exception {
        List<String> ids = zookeeper.getChildren().forPath("/brokers/ids");
        StringBuilder brokerList = new StringBuilder();
        for (String id : ids) {
            String brokerInfo = new String(zookeeper.getData().forPath("/brokers/ids/" + id));
            ZookeeperVO vo = GSON.fromJson(brokerInfo, ZookeeperVO.class);
            if (brokerList.length() == 0) {
                brokerList.append(vo.host + ":" + vo.port);
            } else {
                brokerList.append("," + vo.host + ":" + vo.port);
            }
        }
        config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList.toString());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        config.put(ProducerConfig.ACKS_CONFIG, "1");

        props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList.toString());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka_group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    }

    public KafkaProducer<String, String> createShareProducer(Vertx vertx, String name, KafkaFactory kafkaFactory) {
        return KafkaProducer.createShared(vertx, name, kafkaFactory.getKafkaProduceConfig());
    }


    public KafkaProducer<String, String> createShareProducer(Vertx vertx, String name) {
        return KafkaProducer.createShared(vertx, name, config);
    }

    public KafkaConsumer<String, String> createConsumer(String groupId) {
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return KafkaConsumer.create(vertx, props);
    }

    public Map<String, String> getKafkaProduceConfig() {
        return config;
    }
}
