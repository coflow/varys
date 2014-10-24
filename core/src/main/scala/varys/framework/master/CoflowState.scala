package varys.framework.master

private[varys] object CoflowState extends Enumeration {
  
  type CoflowState = Value

  val WAITING, READY, RUNNING, FINISHED, FAILED, REJECTED = Value
}
