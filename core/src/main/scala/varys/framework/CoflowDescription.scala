package varys.framework

private[varys] class CoflowDescription(
    val name: String,
    val cores: Int,
    val command: Command,
    val varysHome: String)
  extends Serializable {

  val user = System.getProperty("user.name", "<unknown>")

  override def toString: String = "CoflowDescription(" + name + ")"
}
