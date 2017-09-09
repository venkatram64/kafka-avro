package com.venkat.scala.entity

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.GenericRecordBuilder


/**
  * Created by venkatram.veerareddy on 9/8/2017.
  */
class EmployeeKey {

  val parser = new Schema.Parser
  val keySchema = parser.parse(getClass.getResourceAsStream("/schema/EmploeeKeyV0.avsc"))

  private var empId: String = _

  def this(empId: String){
    this()
    this.empId = empId
  }

  def getEmpId: String = empId

  def toRecord(): Record = {
    val key = new GenericRecordBuilder(keySchema)
    .set("empId", empId)
    .build()
    key
  }

}

object EmployeeKey{
  def toSchemaKey(record: Record) = new EmployeeKey(record.get("id").toString)
}