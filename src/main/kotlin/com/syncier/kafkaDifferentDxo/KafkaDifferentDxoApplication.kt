package com.syncier.kafkaDifferentDxo

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.annotation.KafkaHandler
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.config.TopicBuilder
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.scheduling.annotation.EnableScheduling
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import org.springframework.stereotype.Service
import java.time.LocalDateTime
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit


const val TOPIC = "allDxo"
val latch = CountDownLatch(2)

@SpringBootApplication
@EnableScheduling
@EnableKafka
class KafkaDifferentDxoApplication {
    @Bean
    fun topic() = TopicBuilder.name(TOPIC).build()
}

data class InventoryDataRequest(val id: Int, val message: String)
data class DiscountTransfer(val id: Int, val discount: Int)

@Service
class MySender(val kafkaTemplate: KafkaTemplate<String, Any>) {
    @Scheduled(fixedRate = 3_000)
    fun cmd() {
        val inventoryDataRequest = InventoryDataRequest(1, "hello world @${LocalDateTime.now()}")
        kafkaTemplate.send(TOPIC, inventoryDataRequest)

        val discountTransfer = DiscountTransfer(2, 100)
        kafkaTemplate.send(TOPIC, discountTransfer)

        println("sent all")
    }
}

@Component
@KafkaListener(topics = [TOPIC], groupId = "class")
class MaKafkaListener {
    @KafkaHandler
    fun receiveInvData(data: InventoryDataRequest) {
        println(">>>in inv data handler $data")
    }

    @KafkaHandler
    fun receiveDiscount(discount: DiscountTransfer) {
        println(">>>in discount: $discount")
    }
}

fun main(args: Array<String>) {
	runApplication<KafkaDifferentDxoApplication>(*args)
}
