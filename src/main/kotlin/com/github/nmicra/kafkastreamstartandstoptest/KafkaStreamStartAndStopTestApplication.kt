package com.github.nmicra.kafkastreamstartandstoptest

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class KafkaStreamStartAndStopTestApplication

fun main(args: Array<String>) {
	runApplication<KafkaStreamStartAndStopTestApplication>(*args)
}
