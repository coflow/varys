package varys.framework

import scala.collection.Map

private[varys] case class Command(
    mainClass: String,
    arguments: Seq[String],
    environment: Map[String, String]) {
}
