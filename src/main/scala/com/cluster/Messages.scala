package com.cluster

/**
 * 
 * Cluster Node Messages
 */
object Messages {
  case class Job(num: BigInt)
  case class JobResult(num: BigInt)
  case class Message(msg: String)
  case object BackendRegistration
}