package varys.framework.client

/**
 * Callbacks invoked by framework client when various events happen. Currently supported events:
 * connecting to the cluster and disconnecting.
 *
 * Users of this API should *not* block inside the callback methods.
 */
trait ClientListener {
  // NOT SAFE to use the Client UNTIL this method is called
  def connected(clientId: String): Unit

  def disconnected(): Unit

  // Only called for deadline-sensitive coflows
  def coflowRejected(coflowId: String, rejectMessage: String): Unit = { }
}
