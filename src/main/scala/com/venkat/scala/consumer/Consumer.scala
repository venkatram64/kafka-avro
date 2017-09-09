package com.venkat.scala.consumer

import java.util.{Collections, Properties}

import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient
import com.venkat.scala.config.KafkaDestinationSettings
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}

/**
  * Created by venkatram.veerareddy on 9/9/2017.
  */
class Consumer {

  private var consumer:KafkaConsumer[Record, Record] = _

  private var kafkaDestinationSettings: KafkaDestinationSettings = _


  def this(kafkaDestinationSettings: KafkaDestinationSettings){
    this()
    this.kafkaDestinationSettings = kafkaDestinationSettings
    val properties = new Properties

    properties.put(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name, "http://localhost:9999")

    consumer = new KafkaConsumer[Record, Record](properties)
    consumer.subscribe(Collections.singletonList(kafkaDestinationSettings.getTopicName))

    while(true){
      val records = consumer.poll(100)
      import scala.collection.JavaConversions
      for (record <- records){

        println("" + record.key + ", " + record.value +" at offset " + record.offset)
      }
    }
  }

  def close : Unit = {
    consumer.close
  }

}
