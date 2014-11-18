package com.cluster

import akka.actor.Actor
import akka.cluster.Cluster
import akka.actor.ActorSystem
import akka.cluster.ClusterEvent.MemberUp
import Messages._
import akka.cluster.ClusterEvent.CurrentClusterState
import akka.cluster.Member
import akka.actor.RootActorPath
import akka.cluster.MemberStatus
import akka.actor.ActorLogging
import com.typesafe.config.ConfigFactory
import akka.actor.Props

class Backend extends Actor with ActorLogging {
  
  val cluster = Cluster(context system)
  
  override def preStart(): Unit = cluster.subscribe(self, classOf[MemberUp])
  override def postStop(): Unit = cluster.unsubscribe(self)
  
  def receive = {
    case Job(num) => log.info("job assigned, computing factorial of {}", num)
      				sender ! Message(s"fac of $num is ${fac(num)}")
      				
    case state: CurrentClusterState => state.members.filter(_.status == MemberStatus.Up) foreach register
    
    case MemberUp(member) => log.info("member {} is up", member.address)
      						register(member)	
  }
  
  def register(member: Member): Unit = {
    if(member.hasRole("frontend")) {
      log.info("Backend registration with frontend also called Master Node: {}", member.address)
      context.actorSelection(RootActorPath(member.address) / "user" / "frontend") ! BackendRegistration
    }
  }
  
  def fac(num: BigInt): BigInt = BigInt(1).to(num, BigInt(1)).foldLeft(BigInt(1))((r, c) => r * c)
}

object Backend extends App {
  val port = if(args.isEmpty) "0" else args(0)
  val config = ConfigFactory.parseString(s"akka.remote.netty.tcp.port=$port").
  				withFallback(ConfigFactory.parseString(s"akka.cluster.roles = [backend]")).
  				withFallback(ConfigFactory.load())
  
  val system = ActorSystem("ClusterSystem", config)
  val backend = system actorOf(Props[Backend], name = "backend")
}