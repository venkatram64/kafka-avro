package com.venkat.scala.config

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Configuration

/**
  * Created by venkatram.veerareddy on 9/8/2017.
  */

@Configuration
@ConfigurationProperties(prefix = "KafkaLocalSettings")
class KafkaDestinationSettings {

  private var bootstrapServiceConfig: String = _
  private var zookeeperUrl: String = _
  private var topicName: String = _

  def this(bootstrapServiceConfig: String, zookeeperUrl: String, topicName: String){
    this()
    this.bootstrapServiceConfig = bootstrapServiceConfig
    this.zookeeperUrl = zookeeperUrl
    this.topicName = topicName
  }

  def getBootstrapServiceConfig: String = bootstrapServiceConfig

  def getZookeeperUrl: String = zookeeperUrl

  def getTopicName: String = topicName
}
