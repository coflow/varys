package varys.framework.master

import akka.actor.ActorRef

private[varys] class ClientInfo(val id: String, val host: String, val driver: ActorRef) { }
