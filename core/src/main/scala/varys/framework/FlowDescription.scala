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
    val maxReceivers: Int,  // Upper-bound on the number of receivers (how long to keep it around?)
    val originHost: String,
    var originCommPort: Int)
  extends Serializable {

  val user = System.getProperty("user.name", "<unknown>")

  override def toString: String = "FlowDescription(" + id + ":" + coflowId + ")"
  
  def updateCommPort(commPort: Int) {
    originCommPort = commPort
  }
}
