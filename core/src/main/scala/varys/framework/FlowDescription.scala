package varys.framework

private[varys] object FlowType extends Enumeration("FAKE", "INMEMORY", "ONDISK") {
  type FlowType = Value

  val FAKE, INMEMORY, ONDISK = Value
}

private[varys] class FlowDescription(
    val id: String,  // Expected to be unique within the coflow
    val coflowId: String,  // Must be a valid coflow
    val flowType: FlowType.FlowType,  // http://www.scala-lang.org/node/7661
    val sizeInBytes: Long,
    val originHost: String)
  extends Serializable {

  val user = System.getProperty("user.name", "<unknown>")

  override def toString: String = "FlowDescription(" + id + ":" + coflowId + ")"
}
