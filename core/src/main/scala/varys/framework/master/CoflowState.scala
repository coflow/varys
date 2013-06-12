package varys.framework.master

private[varys] object CoflowState extends Enumeration("WAITING", "READY", "RUNNING", "FINISHED", "FAILED") {
  type CoflowState = Value

  val WAITING, READY, RUNNING, FINISHED, FAILED = Value

  val MAX_NUM_RETRY = 10
}
