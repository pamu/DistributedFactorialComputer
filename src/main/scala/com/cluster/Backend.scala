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
  
  /**
   * 
   * Get cluster ref from actor system
   */
  val cluster = Cluster(context system)
  
  /**
   * 
   * Subscribe for cluster events 
   */
  override def preStart(): Unit = cluster.subscribe(self, classOf[MemberUp])
  override def postStop(): Unit = cluster.unsubscribe(self)
  
  def receive = {
    /**
     * 
     * Perform the job and send the result to the Sender, WARNING: This should be Performed Asynchronously in the Future to
     * Prevent the actor from becoming deaf to requests
     */
    case Job(num) => log.info("job assigned, computing factorial of {}", num)
      				sender ! Message(s"fac of $num is ${fac(num)}")
    
    /**
     * 
     * Register the cluster nodes with the Frontend node once they are Up
     */
    case state: CurrentClusterState => state.members.filter(_.status == MemberStatus.Up) foreach register
    
    case MemberUp(member) => log.info("member {} is up", member.address)
      						register(member)	
  }
  
  /**
   * 
   * Helps Cluster nodes to register with Front end node
   */
  def register(member: Member): Unit = {
    if(member.hasRole("frontend")) {
      log.info("Backend registration with frontend also called Master Node: {}", member.address)
      context.actorSelection(RootActorPath(member.address) / "user" / "frontend") ! BackendRegistration
    }
  }
  
  /**
   * 
   * Amazingly crisp code using FoldLeft magic
   */
  def fac(num: BigInt): BigInt = BigInt(1).to(num, BigInt(1)).foldLeft(BigInt(1))((r, c) => r * c)
}

/**Program to Bootstrap the Cluster Node in the beginning
 * 
 * 
 */
object Backend extends App {
  val port = if(args.isEmpty) "0" else args(0)
  
  /**
   * 
   * Configuration of the Cluster node with Fallbacks
   */
  val config = ConfigFactory.parseString(s"akka.remote.netty.tcp.port=$port").
  				withFallback(ConfigFactory.parseString(s"akka.cluster.roles = [backend]")).
  				withFallback(ConfigFactory.load())
  
  val system = ActorSystem("ClusterSystem", config)
  
  /**
   * 
   * Create the Backend actor
   */
  val backend = system actorOf(Props[Backend], name = "backend")
}