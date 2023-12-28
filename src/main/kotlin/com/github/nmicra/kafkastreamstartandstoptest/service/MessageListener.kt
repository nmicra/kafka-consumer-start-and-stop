package com.github.nmicra.kafkastreamstartandstoptest.service

import com.fasterxml.jackson.module.kotlin.readValue
import com.github.nmicra.kafkastreamstartandstoptest.util.KafkaUtil
import com.github.nmicra.kafkastreamstartandstoptest.util.KafkaUtil.objectMapper
import com.github.nmicra.kafkastreamstartandstoptest.util.Person
import jakarta.annotation.PostConstruct
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Service

//@Service
class MessageListener {


    @PostConstruct
    fun init() {
        println(">>> MessageListener")
    }


    @KafkaListener(topics = ["TEMP_TEST_TOPIC1"])
    fun listen(@Payload message: ByteArray, @Header("currentTime") currentTime: Long) {

        val person: Person = objectMapper.readValue(message)


        // Print the Person object and currentTime header
        println(">>> Received Person: $person")
        println(">>> Current Time Header: $currentTime")
    }
}