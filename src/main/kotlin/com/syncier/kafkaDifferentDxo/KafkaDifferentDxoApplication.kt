package com.syncier.kafkaDifferentDxo

import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.data.web.JsonPath
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.annotation.KafkaHandler
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.config.TopicBuilder
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.converter.ProjectingMessageConverter
import org.springframework.scheduling.annotation.EnableScheduling
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import org.springframework.stereotype.Service
import java.time.LocalDateTime


const val TOPIC = "allDxo"

@SpringBootApplication
@EnableScheduling
@EnableKafka
class KafkaDifferentDxoApplication {
    @Bean
    fun topic() = TopicBuilder.name(TOPIC).build()

    fun myConsumerFactory(kafkaProperties: KafkaProperties) = DefaultKafkaConsumerFactory(
        kafkaProperties.buildConsumerProperties(),
        StringDeserializer(),
        StringDeserializer()
    )

    @Bean
    fun myKafkaListenerContainerFactory(kafkaProperties: KafkaProperties): ConcurrentKafkaListenerContainerFactory<String, String> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, String>()
        factory.consumerFactory = myConsumerFactory(kafkaProperties)
        factory.setMessageConverter(ProjectingMessageConverter())
        return factory
    }
}
data class Request(val id: String, val requestMessage: String)
data class InventoryDataRequest(val id: Int, val message: String, var request: Request? = null)
data class DiscountTransfer(val id: Int, val discount: Int, var request: Request? = null)
interface RequestInterface {
    @JsonPath("$.request")
    fun getRequest(): Request
}

@Service
class MySender(val kafkaTemplate: KafkaTemplate<String, Any>) {
    @Scheduled(fixedRate = 3_000)
    fun cmd() {
        val inventoryDataRequest = InventoryDataRequest(1, "hello world @${LocalDateTime.now()}").apply {
            request=Request("requestId${this.id}", "requestMessage${this.message}")
        }
        kafkaTemplate.send(TOPIC, inventoryDataRequest)

        val discountTransfer = DiscountTransfer(2, 100).apply {
            request=Request("requestId${this.id}", "requestMessage${this.discount}")
        }
        kafkaTemplate.send(TOPIC, discountTransfer)

        println("sent all")
    }
}

@Component
class MyListener() {
    @KafkaListener(groupId = "method", topics = [TOPIC], containerFactory = "myKafkaListenerContainerFactory")
    fun listen(data: RequestInterface) {
        println(">>> in listen method ${data.getRequest()}")
    }
}

@Component
@KafkaListener(topics = [TOPIC], groupId = "class", containerFactory="kafkaListenerContainerFactory")
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
