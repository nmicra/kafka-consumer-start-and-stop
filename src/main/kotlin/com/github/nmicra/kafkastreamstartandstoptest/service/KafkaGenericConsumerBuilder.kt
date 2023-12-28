package com.github.nmicra.kafkastreamstartandstoptest.service

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.apache.kafka.clients.consumer.*
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import java.time.Duration
import java.time.LocalDateTime
import java.util.*

@Service
class KafkaGenericConsumerBuilder(
    @Value("\${spring.kafka.consumer.bootstrap-servers}") val consumerBootstrapAddress: String,
//    @Value("\${spring.kafka.consumer.max-poll-records}") val maxPollRecords : Int,
//    @Value("\${spring.kafka.consumer.timeout-poll-millis}") val timeoutPollsMillis : Long
) {

    private val timeoutPollsMillis : Long = 250
    private val logger: Logger = LoggerFactory.getLogger(this::class.java)
    private val maxPollRecords : Int = 10000

    fun getConsumer(bootstrapServers : String = consumerBootstrapAddress, topic : String="YOUR_TOPIC", groupId : String = "BatchGenericConsumer-${LocalDateTime.now()}"): Consumer<String, String> {
        val consumer : Consumer<String, String> = KafkaConsumer(consumerConfig(bootstrapServers,groupId))
        consumer.subscribe(arrayListOf(topic))
        return consumer
    }



    private fun consumerConfig(bootstrapServers: String, groupId : String): Properties {
        val config = Properties()
        config.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
        config.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
        config.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        config.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        config.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.LATEST.name.toLowerCase())
        config.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG , maxPollRecords.toString())
        return config
    }


    /**
     * Consumes all the records from a Kafka Consumer with a specified timeout duration and emits them as a Flow.
     * Consummations of the records is from the latest offset, once the consumer started.
     *
     * @param consumer The Kafka Consumer instance to consume records from.
     * @param durationInSeconds The duration in seconds for which the consumer should poll for records.
     * @return A Flow of ConsumerRecords representing the consumed records.
     */
    suspend fun consumeAllWithTimeoutToFlow(consumer : Consumer<String, String>, durationInSeconds: Long): Flow<ConsumerRecord<String, String>>
            = flow {
        var startTime = System.currentTimeMillis()
        do {
            val records = consumer.poll(Duration.ofMillis(timeoutPollsMillis))
            logger.info(">>> size: ${records.toList().size}")
            records.forEach { emit(it) }
        } while (System.currentTimeMillis() - startTime <= durationInSeconds*1000 )

        logger.info(">>> ${LocalDateTime.now()} KafkaGenericConsumerBuilder Timeout occurred - ${consumer.groupMetadata().groupId()}")
        consumer.close()
    }


}