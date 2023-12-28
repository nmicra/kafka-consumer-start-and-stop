package com.github.nmicra.kafkastreamstartandstoptest.service

import com.fasterxml.jackson.module.kotlin.readValue
import com.github.nmicra.kafkastreamstartandstoptest.util.KafkaUtil
import com.github.nmicra.kafkastreamstartandstoptest.util.Person
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.KStream
import org.springframework.stereotype.Service
import java.util.*
import java.util.concurrent.ConcurrentHashMap

@Service
class KafkaStreamService {

    private val activeStreams: ConcurrentHashMap<String, KafkaStreams> = ConcurrentHashMap()
    val consumedPersons = mutableListOf<Person>()

    val kafkaAdminClient: AdminClient by lazy {  AdminClient.create(streamProperties())}

    fun startStreaming(topicName: String) {
        // Check if a stream for this topic already exists
        if (activeStreams.containsKey(topicName)) {
            println("Stream for topic $topicName already exists.")
            return
        }

        // Define the stream processing logic
        val builder = StreamsBuilder()
        val personStream: KStream<String, ByteArray> = builder.stream(topicName)
        personStream.foreach { key, value -> println("####### Key: $key, Value: $value") }
       /* personStream.foreach { key, value -> run {
            println("... consumed : $key")
            val person: Person = KafkaUtil.objectMapper.readValue(value)
            consumedPersons.add(person)
        } }*/




        // Build and start the stream
        val streams = KafkaStreams(builder.build(), streamProperties())
        streams.start()
        activeStreams[topicName] = streams

        println(">>> Started streaming for topic $topicName.")
    }

    fun stopStreaming(topicName: String) {
        activeStreams[topicName]?.let {
            it.close()
            activeStreams.remove(topicName)
            println("Stopped streaming for topic $topicName.")
        } ?: println("No active stream for topic $topicName.")
    }

    private fun streamProperties(): Properties {
        val props = Properties()
        props["bootstrap.servers"] = "localhost:9092"
        props["application.id"] = "kafka-streams-service"
        props["auto.offset.reset"] = "latest"
        props["default.key.serde"] = Serdes.String().javaClass.name
        props["default.value.serde"] = Serdes.String().javaClass.name
        // Add more properties as needed
        return props
    }
}