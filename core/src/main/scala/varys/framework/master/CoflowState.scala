package varys.framework.master

private[varys] object CoflowState extends Enumeration(
    "WAITING", 
    "READY", 
    "RUNNING", 
    "FINISHED", 
    "FAILED", 
    "REJECTED") {
  
  type CoflowState = Value

  val WAITING, READY, RUNNING, FINISHED, FAILED, REJECTED = Value
}
