package varys.framework.master

private[varys] object JobState extends Enumeration("WAITING", "RUNNING", "FINISHED", "FAILED") {
  type JobState = Value

  val WAITING, RUNNING, FINISHED, FAILED = Value

  val MAX_NUM_RETRY = 10
}
