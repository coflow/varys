package varys.framework.scheduler

private[scheduler] case class Coflow(coflowId: String) {
  var currentJobQueue: Int = 0
  var sizeSoFar: Long = 0

  override def toString: String = "Coflow(" + coflowId + ", " + "currentJobQueue="+ 
    currentJobQueue + ", " + "sizeSoFar=" + sizeSoFar + ")"
}
