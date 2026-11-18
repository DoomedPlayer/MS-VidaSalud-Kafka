package com.BinarySeint.vsKafka.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.BinarySeint.vsKafka.service.KafkaAdminService;

@Configuration
public class KafkaAdminConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaAdminConfig.class);

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${vidasalud.kafka.auto-init-topology:true}")
    private boolean autoInitTopology;

    @Bean(destroyMethod = "close")
    public AdminClient kafkaAdminClient() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configs.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);
        configs.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 15000);
        return AdminClient.create(configs);
    }

    @Bean
    public ApplicationRunner topologyInitializer(KafkaAdminService kafkaAdminService) {
        return args -> {
            if (autoInitTopology) {
                try {
                    log.info("🚀 Verificando y aprovisionando topología Kafka del Caso VidaSalud...");
                    kafkaAdminService.inicializarTopologiaVidaSalud();
                } catch (Exception e) {
                    log.warn("⚠️ No se pudo inicializar la topología al arrancar (puedes ejecutar POST /api/kafka-admin/topics/init-topology manualmente): {}", e.getMessage());
                }
            }
        };
    }
}