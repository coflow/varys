package varys.framework

object CoflowType extends Enumeration {
  type CoflowType = Value

  val DEFAULT, SHUFFLE, BROADCAST, INCAST, ANYCAST = Value
}

class CoflowDescription(
    val name: String,
    val coflowType: CoflowType.CoflowType,  // http://www.scala-lang.org/node/7661
    val maxFlows: Int,  // Upper-bound on the number of flows
    val maxSizeInBytes: Long,  // Upper-bound on coflow size
    val deadlineMillis: Long = 0)  // Greater than 0 is valid deadline
  extends Serializable {

  val user = System.getProperty("user.name", "<unknown>")

  override def toString: String = "CoflowDescription(" + name + "["+ coflowType + ", " + 
    maxFlows + "])"
}
