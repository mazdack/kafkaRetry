package com.syncier.kafkaDifferentDxo

import com.fasterxml.jackson.annotation.JsonFormat
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
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
import java.time.Duration
import java.time.LocalDateTime
import java.time.LocalDateTime.now
import java.time.temporal.ChronoUnit
import java.time.temporal.TemporalAmount
import kotlin.random.Random


const val INVENTORY_TOPIC = "inventory"
const val DISCOUNT_TOPIC = "discount"
const val RETRY_TOPIC = "retry"
const val DEAD_TOPIC = "dead"
const val MAX_RETRIES = 5

var lastProcessedId = 0

@SpringBootApplication
@EnableKafka
class KafkaDifferentDxoApplication {
    @Bean
    fun inventoryTopic() = TopicBuilder.name(INVENTORY_TOPIC).build()

    @Bean
    fun discountTopic() = TopicBuilder.name(DISCOUNT_TOPIC).build()

    @Bean
    fun retryTopic() = TopicBuilder.name(RETRY_TOPIC).build()

    @Bean
    fun deadTopic() = TopicBuilder.name(DEAD_TOPIC).build()

    @Bean
    fun retryingErrorHandler(kafkaTemplate: KafkaTemplate<String, Any>, @Value("\${app.spring.retry.timeout}") retryTimeout: Duration): ErrorHandler {
        return object: ErrorHandler {
            override fun handle(thrownException: Exception, data: ConsumerRecord<*, *>) {
                val logger = LoggerFactory.getLogger("retryingErrorHandler")
                logger.info("handling exception ${thrownException.cause?.message} for ${data.value()}")

                val request = data.value() as RetryableRequest
                request.incrementTimeout(retryTimeout)

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
    fun deadLetterErrorHandler(kafkaTemplate: KafkaTemplate<String, Any>): ErrorHandler {
        return object: ErrorHandler {
            override fun handle(thrownException: Exception, data: ConsumerRecord<*, *>) {
                val logger = LoggerFactory.getLogger("deadLetterErrorHandler")
                logger.info("handling exception ${thrownException.cause?.message} for ${data.value()}")

                val message = MessageBuilder
                    .withPayload(data.value())
                    .setHeader(KafkaHeaders.TOPIC, DEAD_TOPIC)
                    .setHeader(KafkaHeaders.REPLY_TOPIC, data.headers().lastHeader(KafkaHeaders.REPLY_TOPIC))
                    .build()

                kafkaTemplate.send(message)

                logger.info("sent ${data.value()} to $DEAD_TOPIC topic")
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

    @Bean
    fun retryKafkaListenerContainerFactory(kafkaProperties: KafkaProperties, consumerFactory: ConsumerFactory<Any, Any>, deadLetterErrorHandler: ErrorHandler,
                                           kafkaTemplate: KafkaTemplate<String, Any>): ConcurrentKafkaListenerContainerFactory<Any, Any> {
        val factory = ConcurrentKafkaListenerContainerFactory<Any, Any>()
        factory.consumerFactory = consumerFactory
        factory.setErrorHandler(deadLetterErrorHandler)
        factory.setReplyTemplate(kafkaTemplate)
        return factory
    }
}

interface RetryableRequest {
    fun incrementTimeout(delta: TemporalAmount)
    fun incrementRetries()

    fun retryAt(): LocalDateTime
    fun retries(): Int
}

data class Request(val id: String, var requested: Int = 1, @JsonFormat(shape = JsonFormat.Shape.STRING) var retryAt: LocalDateTime = now()): RetryableRequest {
    override fun incrementTimeout(delta: TemporalAmount) {
        retryAt = retryAt.plus(delta)
    }

    override fun incrementRetries() {
        requested++
    }

    override fun retryAt() = retryAt
    override fun retries() = requested
}
data class InventoryDataRequest(val id: Int, val message: String, var request: Request? = null): RetryableRequest by request!!
data class DiscountTransfer(val id: Int, val discount: Int, var request: Request? = null): RetryableRequest by request!!


@ConditionalOnProperty(
 value = ["app.scheduling.enable"], havingValue = "true", matchIfMissing = true
)
@Configuration
@EnableScheduling
class SchedulingConfiguration {
}

@Service
class MySender(val kafkaTemplate: KafkaTemplate<String, Any>, var id: Int = 1) {
    @Scheduled(fixedRate = 1_000)
    fun cmd() {
        val inventoryDataRequest = InventoryDataRequest(id++, "hello world @${now()}", Request("requestId${id}"))
        kafkaTemplate.send(INVENTORY_TOPIC, inventoryDataRequest)

        val discountTransfer = DiscountTransfer(id++, Random.nextInt(10, 100), Request("requestId${id}"))
        kafkaTemplate.send(DISCOUNT_TOPIC, discountTransfer)

        println(">> sent id $id, last processed id = $lastProcessedId")
    }
}

@Component
class MyListener(val kafkaTemplate: KafkaTemplate<String, Any>) {
    @KafkaListener(groupId = "method", topics = [RETRY_TOPIC], containerFactory="retryKafkaListenerContainerFactory")
    @SendTo
    fun listen(data: RetryableRequest): RetryableRequest? {
        println(">>retry listener started for $data")

        val timeout = now().until(data.retryAt(), ChronoUnit.MILLIS)
        if (timeout > 0) {
            Thread.sleep(timeout)
        }
//        println(">>retry listener will sleep until ${data.retryAt()}")
        data.incrementRetries()

        if (data.retries() <= MAX_RETRIES) {
            println(">>retry listener will retry $data")
            return data
        }

        println(">>retry listener retries exhausted $data")
        throw RetriesExhausted()
    }
}

class RetriesExhausted : Throwable()

@Service
class MyService {
    fun doWork1(inventoryDataRequest: InventoryDataRequest) {
        if (Random.nextBoolean()) {
            throw RuntimeException("oops")
        }
        println(">>>in inv data handler $inventoryDataRequest")
        lastProcessedId = inventoryDataRequest.id
    }

    fun doWork(discount: DiscountTransfer) {
        if (Random.nextBoolean()) {
            throw RuntimeException("oops")
        }
        println(">>>in discount: $discount")
        lastProcessedId = discount.id
    }
}

@Component
@KafkaListener(topics = [INVENTORY_TOPIC, DISCOUNT_TOPIC], groupId = "class", containerFactory="retryingKafkaListenerContainerFactory")
class MaKafkaListener(val myService: MyService) {
    @KafkaHandler
    fun receiveInvData(data: InventoryDataRequest) {
        myService.doWork1(data)
    }

    @KafkaHandler
    fun receiveDiscount(discount: DiscountTransfer) {
        myService.doWork(discount)
    }
}

fun main(args: Array<String>) {
	runApplication<KafkaDifferentDxoApplication>(*args)
}
