package varys.framework.scheduler

import scala.collection.mutable.HashMap

private[scheduler] case class Coflow(coflowId: Int) {
  var currentJobQueue: Int = 0
  var sizeSoFar: Long = 0
  val flows = new HashMap[String, Array[String]]()

  override def toString: String = "Coflow(" + coflowId + ", " + "currentJobQueue="+ 
    currentJobQueue + ", " + "sizeSoFar=" + sizeSoFar + ")"
}
