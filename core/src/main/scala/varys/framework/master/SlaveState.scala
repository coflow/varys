package varys.framework.master

private[varys] object SlaveState extends Enumeration("ALIVE", "DEAD", "DECOMMISSIONED") {
  type SlaveState = Value

  val ALIVE, DEAD, DECOMMISSIONED = Value
}
