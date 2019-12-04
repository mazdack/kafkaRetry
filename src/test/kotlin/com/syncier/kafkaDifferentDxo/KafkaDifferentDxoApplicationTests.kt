package com.syncier.kafkaDifferentDxo

import com.fasterxml.jackson.databind.ObjectMapper
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.whenever
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG
import org.apache.kafka.common.TopicPartition
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.test.EmbeddedKafkaBroker
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.kafka.test.utils.KafkaTestUtils
import java.lang.RuntimeException
import java.time.LocalDateTime
import java.util.concurrent.TimeUnit


@SpringBootTest(properties = ["app.scheduling.enable=false", "app.spring.retry.timeout=10ms"])
@EmbeddedKafka(bootstrapServersProperty = "spring.kafka.bootstrap-servers", topics = [INVENTORY_TOPIC, DEAD_TOPIC], partitions = 1)
@TestInstance(value = PER_CLASS)
class KafkaDifferentDxoApplicationTests {
    @Autowired
    lateinit var embeddedKafka: EmbeddedKafkaBroker
    @Autowired
    lateinit var kafkaTemplate: KafkaTemplate<String, InventoryDataRequest>
    @Autowired
    lateinit var objectMapper: ObjectMapper
    @MockBean
    lateinit var myService: MyService

    lateinit var inventoryDataConsumer: Consumer<String, String>
    lateinit var deadConsumer: Consumer<String, String>

    @BeforeAll
    fun beforeAll() {
        inventoryDataConsumer = createConsumer("testGroup1", INVENTORY_TOPIC)
        deadConsumer = createConsumer("testGroup3", DEAD_TOPIC)
    }

	@Test
	fun `context loads`() {
    }

    @Test
    fun `request is retried several times and then request goes to dead topic`() {
        whenever(myService.doWork1(any())).thenThrow(RuntimeException("mocked"))
        kafkaTemplate.send(INVENTORY_TOPIC, InventoryDataRequest(1, "hello world @${LocalDateTime.now()}", Request("requestId1")))

        await().atMost(1, TimeUnit.SECONDS).untilAsserted {
            inventoryDataConsumer.seekToBeginning(listOf(TopicPartition(INVENTORY_TOPIC, 0)))
            val records = KafkaTestUtils.getRecords(inventoryDataConsumer)
            assertThat(records.count()).isEqualTo(MAX_RETRIES)
        }

        await().atMost(1, TimeUnit.SECONDS).untilAsserted {
            deadConsumer.seekToBeginning(listOf(TopicPartition(DEAD_TOPIC, 0)))
            val records = KafkaTestUtils.getRecords(deadConsumer)
            assertThat(records.count()).isEqualTo(1)
            val firstRecordValue = records.records(DEAD_TOPIC).first().value()
            val inventoryDataRequest = objectMapper.readValue(firstRecordValue, InventoryDataRequest::class.java)
            assertThat(inventoryDataRequest.request!!.requested).isEqualTo(MAX_RETRIES+1)
        }
    }

    private fun <V : Any> createConsumer(group: String, topic: String): Consumer<String, V> {
        val consumerProps = KafkaTestUtils.consumerProps(group, "true", embeddedKafka)
        consumerProps[AUTO_OFFSET_RESET_CONFIG] = "earliest"
        val cf: ConsumerFactory<String, V> = DefaultKafkaConsumerFactory(consumerProps)
        val consumer: Consumer<String, V> = cf.createConsumer()
        embeddedKafka.consumeFromAnEmbeddedTopic(consumer, topic)
        return consumer
    }
}
