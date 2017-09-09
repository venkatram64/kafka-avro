package com.venkat.scala.service

import com.venkat.scala.config.KafkaDestinationSettings
import com.venkat.scala.consumer.Consumer
import com.venkat.scala.producer.Producer
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

import scala.collection.mutable

/**
  * Created by venkatram.veerareddy on 9/9/2017.
  */

@Service
class MyService {

  @Autowired
  private var kafkaDestinationSettings: KafkaDestinationSettings = _

  def init: Unit = {

    val publisher = new Producer(kafkaDestinationSettings)
    publisher.produce

    val consumer = new Consumer(kafkaDestinationSettings)
    consumer.run
  }
}
