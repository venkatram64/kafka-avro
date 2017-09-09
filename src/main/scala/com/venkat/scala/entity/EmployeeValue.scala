package com.venkat.scala.entity

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.GenericRecordBuilder

/**
  * Created by venkatram.veerareddy on 9/8/2017.
  */
class EmployeeValue {

  val parser = new Schema.Parser
  val valueSchema = parser.parse(getClass.getResourceAsStream("/schema/EmploeeKeyValue.avsc"))

  private var firstName: String = _
  private var email: String = _

  def this(firstName: String, email: String){
    this()
    this.firstName = firstName
    this.email = email
  }

  def getFirstName: String = firstName
  def getEmail: String = email

  def toRecord : Record = {

    var key = new GenericRecordBuilder(valueSchema)
    .set("firstName", firstName)
    .set("email", email)
    .build()

    key
  }

}

object EmployeeValue {
  def toSchema(record: Record) =
    new EmployeeValue(record.get("firstName").toString, record.get("lastName").toString)
}
