package com.github.nmicra.kafkastreamstartandstoptest

import com.github.nmicra.kafkastreamstartandstoptest.service.KafkaGenericConsumerBuilder
import com.github.nmicra.kafkastreamstartandstoptest.service.KafkaStreamService
import com.github.nmicra.kafkastreamstartandstoptest.util.KafkaUtil
import com.github.nmicra.kafkastreamstartandstoptest.util.Person
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.onCompletion
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.context.TestPropertySource
import org.springframework.test.context.junit.jupiter.SpringExtension
import java.nio.charset.StandardCharsets
import java.time.LocalDateTime
import java.util.*
import java.util.concurrent.atomic.AtomicInteger


@EmbeddedKafka(topics = ["TEMP_TEST_TOPIC1","TEMP_TEST_TOPIC2"], partitions = 16)
@TestPropertySource(properties = ["spring.kafka.producer.bootstrap-servers=\${spring.embedded.kafka.brokers}", "spring.kafka.consumer.bootstrap-servers=\${spring.embedded.kafka.brokers}", "spring.kafka.admin.properties.bootstrap.servers=\${spring.embedded.kafka.brokers}"])
@ExtendWith(SpringExtension::class)
@SpringBootTest(webEnvironment= SpringBootTest.WebEnvironment.RANDOM_PORT, classes = [KafkaStreamStartAndStopTestApplication::class])
class KafkaStreamStartAndStopTestApplicationTests {

	@Autowired
	lateinit var kafkaTemplate: KafkaTemplate<String, ByteArray>

	@Autowired
	lateinit var kafkaStreamService : KafkaStreamService

	@Autowired
	lateinit var kafkaGenericConsumerBuilder: KafkaGenericConsumerBuilder



	@Test
	fun streamWithGenericConsumerAfterConsumer() {

		CoroutineScope(Dispatchers.IO).launch {
			repeat(1000) {// sends messages for 10 sec
				val personAsBytes = KafkaUtil.convertToByteArray(Person(it,"kuku-${it}"))
				// Create a ProducerRecord with headers
				val record = ProducerRecord<String, ByteArray>("TEMP_TEST_TOPIC2", UUID.randomUUID().toString(), personAsBytes)
				record.headers().add(RecordHeader("currentTime", System.currentTimeMillis().toString().toByteArray(
					StandardCharsets.UTF_8)))

				// Send the message
				val rs = kafkaTemplate.send(record)
				println("... sent : ${rs.isCompletedExceptionally}")
				runBlocking { delay(10) }
			}
		}

		runBlocking { delay(1000) }
		CoroutineScope(Dispatchers.IO).launch {// Consumes for 3 seconds
			val counter1 = AtomicInteger(0)
			val consumer = kafkaGenericConsumerBuilder.getConsumer(topic = "TEMP_TEST_TOPIC2", groupId = "grp")

			val flow = kafkaGenericConsumerBuilder.consumeAllWithTimeoutToFlow(consumer, 3)
			flow
				.onCompletion {
					consumer.close()
					println(">>> Finished consumer1 consumed ${counter1.get()} records")
				}
				.collect { record ->
					val person = KafkaUtil.objectMapper.readValue(record.value(), Person::class.java)
					println(">>> ${LocalDateTime.now()} person is : $person, partition/offset ${record.partition()}/${record.offset()}")
					counter1.incrementAndGet()
					kafkaStreamService.consumedPersons.add(person)
				}
		}

		runBlocking { delay(3000) }
		kafkaStreamService.kafkaAdminClient.deleteConsumerGroups(listOf("grp"))
		println(">>>> ConsumerGroup deleted.")
		runBlocking { delay(1000) }

		CoroutineScope(Dispatchers.IO).launch { // Second Consumer for 3 seconds
			val counter1 = AtomicInteger(0)
			val consumer = kafkaGenericConsumerBuilder.getConsumer(topic = "TEMP_TEST_TOPIC2", groupId = "grp2")

			val flow = kafkaGenericConsumerBuilder.consumeAllWithTimeoutToFlow(consumer, 3)
			flow
				.onCompletion {
					consumer.close()
					println(">>> Finished consumer2 consumed ${counter1.get()} records")
				}
				.collect { record ->
					val person = KafkaUtil.objectMapper.readValue(record.value(), Person::class.java)
					println(">>>> ${LocalDateTime.now()} person is : $person ,partition/offset ${record.partition()}/${record.offset()}")
					counter1.incrementAndGet()
					kafkaStreamService.consumedPersons.add(person)
				}
		}
		runBlocking { delay(10000) }
		kafkaStreamService.kafkaAdminClient.deleteConsumerGroups(listOf("grp2"))

		assert(kafkaStreamService.consumedPersons.isNotEmpty())

		println(">>> streamTest1")
	}

	@Test
	fun streamWithGenericConsumerConcurrent() {

		/**
		 * Consumer1, and Consumer2, each consumes for 4 seconds
		 * Consumer2 will start 1 sec after Consumer1
		 * Some of the messages will be consumed by both
		 */

		CoroutineScope(Dispatchers.IO).launch {
			repeat(1000) {// sends messages for 10 sec
				val personAsBytes = KafkaUtil.convertToByteArray(Person(it,"kuku-${it}"))
				// Create a ProducerRecord with headers
				val record = ProducerRecord<String, ByteArray>("TEMP_TEST_TOPIC2", UUID.randomUUID().toString(), personAsBytes)
				record.headers().add(RecordHeader("currentTime", System.currentTimeMillis().toString().toByteArray(
					StandardCharsets.UTF_8)))

				// Send the message
				val rs = kafkaTemplate.send(record)
				println("... sent : ${rs.isCompletedExceptionally}")
				runBlocking { delay(10) }
			}
		}

		runBlocking { delay(1000) }
		CoroutineScope(Dispatchers.IO).launch {
			val counter1 = AtomicInteger(0)
			val consumer = kafkaGenericConsumerBuilder.getConsumer(topic = "TEMP_TEST_TOPIC2", groupId = "grp")

			val flow = kafkaGenericConsumerBuilder.consumeAllWithTimeoutToFlow(consumer, 4)
			flow
				.onCompletion {
					consumer.close()
					println(">>> Finished consumer1 consumed ${counter1.get()} records")
				}
				.collect { record ->
					val person = KafkaUtil.objectMapper.readValue(record.value(), Person::class.java)
					println(">>> ${LocalDateTime.now()} person is : $person, partition/offset ${record.partition()}/${record.offset()}")
					counter1.incrementAndGet()
					kafkaStreamService.consumedPersons.add(person)
				}
		}

		runBlocking { delay(1000) }

		CoroutineScope(Dispatchers.IO).launch { // Second launch should continue
			val counter1 = AtomicInteger(0)
			val consumer = kafkaGenericConsumerBuilder.getConsumer(topic = "TEMP_TEST_TOPIC2", groupId = "grp2")

			val flow = kafkaGenericConsumerBuilder.consumeAllWithTimeoutToFlow(consumer, 4)
			flow
				.onCompletion {
					consumer.close()
					println(">>> Finished consumer2 consumed ${counter1.get()} records")
				}
				.collect { record ->
					val person = KafkaUtil.objectMapper.readValue(record.value(), Person::class.java)
					println(">>>> ${LocalDateTime.now()} person is : $person ,partition/offset ${record.partition()}/${record.offset()}")
					counter1.incrementAndGet()
					kafkaStreamService.consumedPersons.add(person)
				}
		}
		runBlocking { delay(10000) }

		println(">>>> ConsumerGroup deleted.")
		kafkaStreamService.kafkaAdminClient.deleteConsumerGroups(listOf("grp"))
		kafkaStreamService.kafkaAdminClient.deleteConsumerGroups(listOf("grp2"))

		assert(kafkaStreamService.consumedPersons.isNotEmpty())

		println(">>> streamTest1")
	}

}
