package varys.framework

object CoflowType extends Enumeration("DEFAULT", "SHUFFLE", "BROADCAST", "INCAST", "ANYCAST") {
  type CoflowType = Value

  val DEFAULT, SHUFFLE, BROADCAST, INCAST, ANYCAST = Value
}

class CoflowDescription(
    val name: String,
    val coflowType: CoflowType.CoflowType,  // http://www.scala-lang.org/node/7661
    val maxFlows: Int)  // Upper-bound on the number of flows
  extends Serializable {

  val user = System.getProperty("user.name", "<unknown>")

  override def toString: String = "CoflowDescription(" + name + ")"
}
