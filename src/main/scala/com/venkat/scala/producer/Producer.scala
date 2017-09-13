package com.venkat.scala.producer

import java.util.Properties

import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient
import com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroDeserializer
import com.venkat.scala.config.KafkaDestinationSettings
import com.venkat.scala.entity.{EmployeeKey, EmployeeValue}
import org.apache.avro.generic.GenericData.Record
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
  * Created by venkatram.veerareddy on 9/9/2017.
  */
class Producer {

  private var producer: KafkaProducer[Record, Record] = _

  private var kafkaDestinationSettings: KafkaDestinationSettings = _


  def this(kafkaDestinationSettings: KafkaDestinationSettings){
    this()

    val properties = new Properties()

    properties.put("zookeeper.connect", kafkaDestinationSettings.getZookeeperUrl)
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaDestinationSettings.getBootstrapServiceConfig)
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[KafkaAvroDeserializer].getName)
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[KafkaAvroDeserializer].getName)
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    properties.put("group.id", "xyz")

    properties.put(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name, "http://localhost:9999/api/v1")
  }

  def produce = {
    val empKey: EmployeeKey = new EmployeeKey("999")
    val empValue: EmployeeValue = new EmployeeValue("Venkatram", "v.v.com")
    val key = empKey.toRecord()
    val value = empValue.toRecord

    producer.send(new ProducerRecord[Record, Record](kafkaDestinationSettings.getTopicName,key, value))

  }

}
