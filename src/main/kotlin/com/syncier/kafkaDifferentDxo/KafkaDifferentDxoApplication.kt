package com.syncier.kafkaDifferentDxo

import com.fasterxml.jackson.annotation.JsonFormat
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.annotation.KafkaHandler
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.config.TopicBuilder
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.listener.ErrorHandler
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.handler.annotation.SendTo
import org.springframework.messaging.support.MessageBuilder
import org.springframework.scheduling.annotation.EnableScheduling
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import org.springframework.stereotype.Service
import java.time.LocalDateTime
import java.time.LocalDateTime.now
import java.time.temporal.ChronoUnit
import kotlin.random.Random


const val TOPIC = "allDxo"
const val RETRY_TOPIC = "retry"
const val RETRY_TIMEOUT_SEC = 5L

var lastProcessedId = 0

@SpringBootApplication
@EnableScheduling
@EnableKafka
class KafkaDifferentDxoApplication {
    @Bean
    fun topic() = TopicBuilder.name(TOPIC).build()

    @Bean
    fun retryTopic() = TopicBuilder.name(RETRY_TOPIC).build()

    @Bean
    fun retryingErrorHandler(kafkaTemplate: KafkaTemplate<String, Any>): ErrorHandler {
        return object: ErrorHandler {
            override fun handle(thrownException: Exception, data: ConsumerRecord<*, *>?) {
                println("handling exception ${thrownException.cause!!.message} for ${data?.value()}")

                val request = data!!.value() as RetryableRequest
                request.incrementTimeout()

                val message = MessageBuilder
                    .withPayload(request)
                    .setHeader(KafkaHeaders.TOPIC, RETRY_TOPIC)
                    .setHeader(KafkaHeaders.REPLY_TOPIC, data.topic())
                    .build()

                kafkaTemplate.send(message)
            }

            override fun isAckAfterHandle() = true
        }
    }

    @Bean
    fun retryingKafkaListenerContainerFactory(kafkaProperties: KafkaProperties, consumerFactory: ConsumerFactory<Any, Any>, retryingErrorHandler: ErrorHandler): ConcurrentKafkaListenerContainerFactory<Any, Any> {
        val factory = ConcurrentKafkaListenerContainerFactory<Any, Any>()
        factory.consumerFactory = consumerFactory
        factory.setErrorHandler(retryingErrorHandler)
        return factory
    }
}

interface RetryableRequest {
    fun incrementTimeout()
    fun incrementRetries()

    fun retryAt(): LocalDateTime
    fun retries(): Int
}

data class Request(val id: String, var requested: Int = 1, @JsonFormat(shape = JsonFormat.Shape.STRING) var retryAt: LocalDateTime = now()): RetryableRequest {
    override fun incrementTimeout() {
        retryAt.plusSeconds(RETRY_TIMEOUT_SEC)
    }

    override fun incrementRetries() {
        requested++
    }

    override fun retryAt() = retryAt
    override fun retries() = requested
}
data class InventoryDataRequest(val id: Int, val message: String, var request: Request? = null): RetryableRequest by request!!
data class DiscountTransfer(val id: Int, val discount: Int, var request: Request? = null): RetryableRequest by request!!

@Service
class MySender(val kafkaTemplate: KafkaTemplate<String, Any>, var id: Int = 1) {
    @Scheduled(fixedRate = 1_000)
    fun cmd() {
        val inventoryDataRequest = InventoryDataRequest(id++, "hello world @${now()}", Request("requestId${id}"))
        kafkaTemplate.send(TOPIC, inventoryDataRequest)

        val discountTransfer = DiscountTransfer(id++, Random.nextInt(10, 100), Request("requestId${id}"))
        kafkaTemplate.send(TOPIC, discountTransfer)

        println(">> sent id $id, last processed id = $lastProcessedId")
    }
}

@Component
class MyListener(val kafkaTemplate: KafkaTemplate<String, Any>) {
    @KafkaListener(groupId = "method", topics = [RETRY_TOPIC], containerFactory = "kafkaListenerContainerFactory")
    @SendTo
    fun listen(data: RetryableRequest): RetryableRequest? {
        println(">>retry listener started for $data")

        val timeout = now().until(data.retryAt(), ChronoUnit.MILLIS)
        if (timeout > 0) {
            Thread.sleep(timeout)
        }
//        println(">>retry listener will sleep until ${data.retryAt()}")
        data.incrementRetries()

        if (data.retries() < 5) {
            println(">>retry listener will retry $data")
            return data
        }

        println(">>retry listener retries exhausted $data")
        return null
    }
}

@Component
@KafkaListener(topics = [TOPIC], groupId = "class", containerFactory="retryingKafkaListenerContainerFactory")
class MaKafkaListener {
    @KafkaHandler
    fun receiveInvData(data: InventoryDataRequest) {
        if (Random.nextBoolean()) {
            throw RuntimeException("oops")
        }
        println(">>>in inv data handler $data")
        lastProcessedId = data.id
    }

    @KafkaHandler
    fun receiveDiscount(discount: DiscountTransfer) {
        if (Random.nextBoolean()) {
            throw RuntimeException("oops")
        }
        println(">>>in discount: $discount")
        lastProcessedId = discount.id
    }
}

fun main(args: Array<String>) {
	runApplication<KafkaDifferentDxoApplication>(*args)
}
