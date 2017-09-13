package com.venkat.scala.consumer

import java.util.{Collections, Properties}

import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient
import com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroDeserializer
import com.venkat.scala.config.KafkaDestinationSettings
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.{ConsumerConfig,  KafkaConsumer}

/**
  * Created by venkatram.veerareddy on 9/9/2017.
  */
class Consumer {

  private var consumer:KafkaConsumer[Record, Record] = _

  private var kafkaDestinationSettings: KafkaDestinationSettings = _


  def this(kafkaDestinationSettings: KafkaDestinationSettings){
    this()
    this.kafkaDestinationSettings = kafkaDestinationSettings

  }

  def run: Unit = {

    val properties = new Properties
    properties.put("enable.auto.commit", "false")
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaDestinationSettings.getBootstrapServiceConfig)
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[KafkaAvroDeserializer].getName)
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[KafkaAvroDeserializer].getName)
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    properties.put("group.id", "xyz")
    properties.put(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name, "http://localhost:9999/api/v1")

    consumer = new KafkaConsumer[Record, Record](properties)
    consumer.subscribe(Collections.singletonList(kafkaDestinationSettings.getTopicName))

    while(true){
      val records = consumer.poll(100)
      System.out.println("records size " + records.count)
      import scala.collection.JavaConversions._
      for (record <- records){

        println("" + record.key + ", " + record.value +" at offset " + record.offset)
      }
    }
  }

  def close : Unit = {
    consumer.close
  }

}
