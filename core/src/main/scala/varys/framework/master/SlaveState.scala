package varys.framework.master

private[varys] object SlaveState extends Enumeration {
  type SlaveState = Value

  val ALIVE, DEAD, DECOMMISSIONED = Value
}
