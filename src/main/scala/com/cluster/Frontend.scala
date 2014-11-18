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
  var backends = IndexedSeq.empty[ActorRef]
  var jobCounter = 0
  
  def receive = {
    case job: Job if backends.isEmpty =>  sender ! Message("Service is unavialable, try again later")
    
    
    case job: Job =>
      jobCounter + 1
      backends(jobCounter % backends.size) forward job
      
      
    case BackendRegistration if !backends.contains(sender) => 
      context watch sender
      backends = backends :+ sender
    
    case Terminated(backend) => backends = backends filterNot(_ == backend) 
  }
}

object Frontend {
  def main(args: Array[String]): Unit = {
    val port = if(args isEmpty) "0" else args(0)
    val config = ConfigFactory.parseString(s"akka.remote.netty.tcp.port=$port").
    			withFallback(ConfigFactory.parseString(s"akka.cluster.roles = [frontend]")).
    			withFallback(ConfigFactory.load())
    val system = ActorSystem("ClusterSystem", config)
    val frontend = system actorOf(Props[Frontend], name = "frontend")
    
    import system.dispatcher
    
    val counter = new AtomicInteger
    
    implicit val timeout = Timeout(10 seconds)
    
    (frontend ? Job(Random.nextInt(20))) onSuccess {
      case Message(msg) => println(s"Message: msg")
      case JobResult(num) => println(s"Result: $num")
    }
    
    
  }
}