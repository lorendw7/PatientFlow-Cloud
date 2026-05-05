package com.pm.patientservice.kafka;

import com.pm.patientservice.model.Patient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import patient.events.PatientEvent;

@Service
public class KafkaProducer {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducer.class);
    private static final String TOPIC = "patient";

    private final KafkaTemplate<String, byte[]> kafkaTemplate;

    public KafkaProducer(KafkaTemplate<String, byte[]> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendEvent(Patient patient) {
        // Build Protobuf event
        PatientEvent event = PatientEvent.newBuilder()
                .setPatientId(patient.getId().toString())
                .setName(patient.getName())
                .setEmail(patient.getEmail())
                .setEventType("PATIENT_CREATED")
                .build();

        byte[] messageBytes = event.toByteArray();
        log.info("Preparing to send message to Kafka topic: {}, message size: {}", TOPIC, messageBytes.length);

        try {
            // Send and WAIT for completion
            var sendResult = kafkaTemplate.send(TOPIC, messageBytes).get();

            // Success log
            var metadata = sendResult.getRecordMetadata();
            log.info("✅ Successfully sent message to Kafka | Topic: {}, Partition: {}, Offset: {}",
                    metadata.topic(),
                    metadata.partition(),
                    metadata.offset());

        } catch (Exception e) {
            // Full error stack trace (critical for debugging)
            log.error("❌ Failed to send message to Kafka topic: {}", TOPIC, e);
        }
    }
}