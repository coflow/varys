package varys.framework.master

private[varys] object CoflowState extends Enumeration("WAITING", "RUNNING", "FINISHED", "FAILED") {
  type CoflowState = Value

  val WAITING, RUNNING, FINISHED, FAILED = Value

  val MAX_NUM_RETRY = 10
}
