package varys.framework

private[varys] object DataType extends Enumeration {
  type DataType = Value

  val FAKE, INMEMORY, ONDISK = Value
}

private[varys] case class DataIdentifier(
    dataId: String, 
    coflowId: String)

private[varys] class FlowDescription(
    val id: String,  // Expected to be unique within the coflow
    val coflowId: String,  // Must be a valid coflow
    val dataType: DataType.DataType,  // http://www.scala-lang.org/node/7661
    val sizeInBytes: Long,
    val maxReceivers: Int,  // Upper-bound on the number of receivers (how long to keep it around?)
    val originHost: String,
    var originCommPort: Int)
  extends Serializable {

  val dataId = DataIdentifier(id, coflowId)
  val user = System.getProperty("user.name", "<unknown>")

  override def toString: String = "FlowDescription(" + id + ":" + dataType + ":" + coflowId + 
    " # " + sizeInBytes + " Bytes)"
  
  def updateCommPort(commPort: Int) {
    originCommPort = commPort
  }
}

private[varys] class FileDescription(
    val id_ : String,  // Expected to be unique within the coflow
    val pathToFile: String,
    val cId_ : String,  // Must be a valid coflow
    val dataType_ : DataType.DataType,
    val offset : Long,
    val size_ : Long,
    val maxR_ : Int,
    val originHost_ : String,
    val originCommPort_ : Int)
  extends FlowDescription(id_, cId_, dataType_, size_, maxR_, originHost_, originCommPort_) {

  override def toString: String = "FileDescription(" + id + "["+ pathToFile + "]:" + dataType + 
    ":" + coflowId + " # " + sizeInBytes + " Bytes)"
}

private[varys] class ObjectDescription(
    val id_ : String,  // Expected to be unique within the coflow
    val className: String, 
    val cId_ : String,  // Must be a valid coflow
    val dataType_ : DataType.DataType,
    val serializedSize : Long,
    val maxR_ : Int,
    val originHost_ : String,
    val origCommPort_ : Int)
  extends FlowDescription(id_, cId_, dataType_, serializedSize, maxR_, originHost_, origCommPort_) {

  override def toString: String = "ObjectDescription(" + id + "["+ className + "]:" + dataType + 
    ":" + coflowId + " # " + sizeInBytes + " Bytes)"
}
