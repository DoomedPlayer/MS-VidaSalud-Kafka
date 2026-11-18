package com.BinarySeint.vsKafka.controller;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.BinarySeint.vsKafka.dto.ConsumerGroupDetailResponse;
import com.BinarySeint.vsKafka.dto.CreateTopicRequest;
import com.BinarySeint.vsKafka.dto.TopicDetailResponse;
import com.BinarySeint.vsKafka.dto.UpdateTopicConfigRequest;
import com.BinarySeint.vsKafka.service.KafkaAdminService;

@RestController
@RequestMapping("/api/kafka-admin")
public class KafkaAdminController {

    private final KafkaAdminService kafkaAdminService;

    public KafkaAdminController(KafkaAdminService kafkaAdminService) {
        this.kafkaAdminService = kafkaAdminService;
    }

    // ==================== ENDPOINTS DE TÓPICOS ====================

    @PostMapping("/topics/init-topology")
    public ResponseEntity<List<TopicDetailResponse>> inicializarTopologia()
            throws ExecutionException, InterruptedException {
        List<TopicDetailResponse> topicos = kafkaAdminService.inicializarTopologiaVidaSalud();
        return new ResponseEntity<>(topicos, HttpStatus.CREATED);
    }

    @PostMapping("/topics")
    public ResponseEntity<TopicDetailResponse> crearTopico(@RequestBody CreateTopicRequest request)
            throws ExecutionException, InterruptedException {
        TopicDetailResponse creado = kafkaAdminService.crearTopico(request);
        return new ResponseEntity<>(creado, HttpStatus.CREATED);
    }

    @GetMapping("/topics")
    public ResponseEntity<List<TopicDetailResponse>> listarTopicos()
            throws ExecutionException, InterruptedException {
        return ResponseEntity.ok(kafkaAdminService.listarTopicos());
    }

    @GetMapping("/topics/{topicName}")
    public ResponseEntity<TopicDetailResponse> obtenerTopico(@PathVariable String topicName)
            throws ExecutionException, InterruptedException {
        return ResponseEntity.ok(kafkaAdminService.obtenerDetalleTopico(topicName));
    }

    @PutMapping("/topics/{topicName}/config")
    public ResponseEntity<TopicDetailResponse> actualizarConfiguracion(
            @PathVariable String topicName,
            @RequestBody UpdateTopicConfigRequest request)
            throws ExecutionException, InterruptedException {
        return ResponseEntity.ok(kafkaAdminService.actualizarConfiguracionTopico(topicName, request));
    }

    @DeleteMapping("/topics/{topicName}")
    public ResponseEntity<Void> eliminarTopico(@PathVariable String topicName)
            throws ExecutionException, InterruptedException {
        kafkaAdminService.eliminarTopico(topicName);
        return ResponseEntity.noContent().build();
    }

    // ==================== ENDPOINTS DE CONSUMER GROUPS ====================

    @GetMapping("/consumer-groups")
    public ResponseEntity<List<String>> listarConsumerGroups()
            throws ExecutionException, InterruptedException {
        return ResponseEntity.ok(kafkaAdminService.listarConsumerGroups());
    }

    @GetMapping("/consumer-groups/{groupId}")
    public ResponseEntity<ConsumerGroupDetailResponse> inspeccionarConsumerGroup(@PathVariable String groupId)
            throws ExecutionException, InterruptedException {
        return ResponseEntity.ok(kafkaAdminService.inspeccionarConsumerGroup(groupId));
    }

    @DeleteMapping("/consumer-groups/{groupId}")
    public ResponseEntity<Void> eliminarConsumerGroup(@PathVariable String groupId)
            throws ExecutionException, InterruptedException {
        kafkaAdminService.eliminarConsumerGroup(groupId);
        return ResponseEntity.noContent().build();
    }

    // ==================== MANEJO CENTRALIZADO DE ERRORES KAFKA ====================

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Object> handleBadRequest(IllegalArgumentException ex) {
        String mensaje = ex.getMessage() != null ? ex.getMessage() : "Solicitud inválida.";
        return ResponseEntity.badRequest().body(Collections.singletonMap("error", mensaje));
    }

    @ExceptionHandler(IllegalStateException.class)
    public ResponseEntity<Object> handleConflict(IllegalStateException ex) {
        String mensaje = ex.getMessage() != null ? ex.getMessage() : "Conflicto con el estado actual del recurso.";
        return ResponseEntity.status(HttpStatus.CONFLICT).body(Collections.singletonMap("error", mensaje));
    }

    @ExceptionHandler(ExecutionException.class)
    public ResponseEntity<Object> handleKafkaExecutionError(ExecutionException ex) {
        Throwable cause = ex.getCause();
        if (cause instanceof UnknownTopicOrPartitionException) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(Collections.singletonMap("error", "El tópico especificado no existe en el clúster Kafka."));
        }
        String detalle = (cause != null && cause.getMessage() != null) ? cause.getMessage() : ex.getMessage();
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Collections.singletonMap("error", "Error de administración Kafka: " + detalle));
    }
}