package varys.framework.client

/**
 * Callbacks invoked by framework client when various events happen. Currently supported events:
 * connecting to the cluster and disconnecting.
 *
 * Users of this API should *not* block inside the callback methods.
 */
private[varys] trait ClientListener {
  def connected(jobId: String): Unit

  def disconnected(): Unit
}
