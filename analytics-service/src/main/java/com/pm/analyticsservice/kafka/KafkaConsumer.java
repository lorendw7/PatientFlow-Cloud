package com.pm.analyticsservice.kafka;

import com.google.protobuf.InvalidProtocolBufferException;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import patient.events.PatientEvent;

@Service
public class KafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);

    // 关键：启动时打印日志，证明消费者被Spring加载了
    @PostConstruct
    public void init() {
        log.info("✅ KafkaConsumer initialized successfully and is ready to listen!");
    }

    // 正确的Kafka监听
    @KafkaListener(
            topics = "patient",
            groupId = "analytics-service",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeEvent(byte[] message) {
        // 只要收到消息，一定会打印这行
        log.info("✅ CONSUMER RECEIVED RAW MESSAGE | Length: {} bytes", message.length);

        try {
            PatientEvent patientEvent = PatientEvent.parseFrom(message);
            log.info("✅ Received Patient Event | ID: {}, Name: {}, Email: {}",
                    patientEvent.getPatientId(),
                    patientEvent.getName(),
                    patientEvent.getEmail());
        } catch (InvalidProtocolBufferException e) {
            log.error("❌ Failed to deserialize Protobuf message: {}", e.getMessage(), e);
        }
    }
}