package com.github.nmicra.kafkastreamstartandstoptest.util

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.Message
import org.springframework.messaging.support.MessageBuilder

object KafkaUtil {

    val objectMapper = jacksonObjectMapper().apply {
        configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        registerModule(JavaTimeModule())
    }


    fun <T> convertToByteArray(eventData: T): ByteArray {
        return objectMapper.writeValueAsBytes(eventData)
    }
}

data class Person(val age : Int, val name : String)