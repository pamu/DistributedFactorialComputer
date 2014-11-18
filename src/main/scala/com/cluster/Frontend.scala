package com.cluster

import akka.actor.Actor
import Messages._
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.Terminated
import com.typesafe.config.ConfigFactory
import akka.actor.ActorSystem
import akka.actor.Props
import java.util.concurrent.atomic.AtomicInteger
import akka.pattern.ask
import scala.util.Random
import akka.util.Timeout
import scala.concurrent.duration._

class Frontend extends Actor with ActorLogging {
  /**
   * 
   * backends is a sq of ActorRef of cluster nodes registered
   */
  var backends = IndexedSeq.empty[ActorRef]
  /**
   * 
   * jobCounter to keep track of number of jobs submitted till now, also helps to distribute work among nodes
   */
  var jobCounter = 0
  
  def receive = {
    /**
     * 
     * If backends are not registered then there is no way to perform the Job, So, send the Client This message
     */
    case job: Job if backends.isEmpty =>  sender ! Message("Service is unavialable, try again later")
    
    /**
     * 
     * Forward the job as is to the Cluster jobs for computation
     */
    case job: Job =>
      log.info("Forwaring the Job to one of the nodes, {}", job)
      jobCounter += 1
      backends(jobCounter % backends.size) forward job
      
    /**
     * 
     * Cluster nodes send BackendRegistration to register themselves
     */
    case BackendRegistration if !backends.contains(sender) => 
      log.info("Registration successful cluster node {}", sender.path)
      context watch sender
      backends = backends :+ sender
    
    /**
     * 
     * Cluster Node which frontend actor is watching might have become unreachable
     */
    case Terminated(backend) => log.info("Looks like {} is terminated", backend.path)
      							backends = backends filterNot(_ == backend) 
  }
}

/**
 * 
 * Program to Bootstrap the Frontend cluster node
 */
object Frontend {
  def main(args: Array[String]): Unit = {
    
    /**
     * 
     * Take port from command line else use "0"
     */
    val port = if(args isEmpty) "0" else args(0)
    val config = ConfigFactory.parseString(s"akka.remote.netty.tcp.port=$port").
    			withFallback(ConfigFactory.parseString(s"akka.cluster.roles = [frontend]")).
    			withFallback(ConfigFactory.load())
    val system = ActorSystem("ClusterSystem", config)
    val frontend = system actorOf(Props[Frontend], name = "frontend")
    
    import system.dispatcher
    
    /**
     * 
     * Java util Concurrent package Atomic Integer Thread safe
     * Use this because Atomic Integer is being shared across threads
     */
    val counter = new AtomicInteger
    
    /**
     * 
     * Timeout once frontend node doesn't respond for 10 seconds
     */
    implicit val timeout = Timeout(10 seconds)
    
    /**
     * 
     * Submit jobs to the frontend actor using Ask pattern
     */
    def block = (frontend ? Job(Random.nextInt(20))) onSuccess {
      case result => result match {
        case Message(msg) => println(s"Message: $msg")
        case JobResult(num) => println(s"Result: $num")  
        case _ => println("Unknown message has been received")
      }
    }
    
    /**
     * 
     * Submit jobs every 3 seconds
     */
    system.scheduler.schedule(2 seconds, 3 seconds) {
      println(s"Job${counter.getAndIncrement()} submitted")
      block
    }
    
  }
}