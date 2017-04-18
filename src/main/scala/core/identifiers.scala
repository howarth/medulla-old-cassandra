package main.scala.core

/*
TODO: Why doesn't this work?
final class IdDecimal(val bd : BigDecimal, override val mc : MathContext) extends BigDecimal(bd, mc){
  def this(bd : BigDecimal) = this(bd, BigDecimal.defaultMathContext)
}
*/
object IdUtils {
  def truncatedBigDecimal(bd: BigDecimal): BigDecimal = {
    BigDecimal(bd.underlying.stripTrailingZeros())
  }
}

trait Id {
  val id : String
  override def toString: String = this.id
  override def equals(that: Any): Boolean = {
    this.getClass == that.getClass && this.toString == that.toString
  }
  override def hashCode(): Int = this.id.hashCode
}

// Use if String Id becomes more complicated
// trait StringId(val id: String)
trait DataId extends Id
class TimeSeriesId(val id : String) extends DataId
class TimeSeriesChannelId( val id : String) extends DataId
class SingleChannelTimeSeriesId(val id: String) extends DataId
class MultiChannelTimeSeriesId(val id: String) extends DataId
class ScalarId(val id : String) extends DataId
class VectorId(val id : String) extends DataId
class Matrix2DId(val id : String) extends DataId

object ScalarId{def apply(id: String) = new ScalarId(id)}
object VectorId{def apply(id: String) = new VectorId(id)}
object Matrix2DId{def apply(id: String) = new Matrix2DId(id)}

trait MetadataAttrId extends Id
class SubjectId(val id: String) extends MetadataAttrId
class ExperimentId(val id: String) extends MetadataAttrId
class BlockId(val id: String) extends MetadataAttrId
class Stimulus(val id: String) extends MetadataAttrId
class StimuliSetId(val id: String) extends MetadataAttrId
// Preprocessing Readable ID
class ProcessSlugId(val id: String) extends MetadataAttrId

// timeString is string of epoch
class Timestamp(t : BigDecimal) extends Ordered[Timestamp] {
  val underlyingBD: BigDecimal = IdUtils.truncatedBigDecimal(t)

  override def equals(that: Any): Boolean = this.getClass == that.getClass && underlyingBD.equals(that)
  override def hashCode: Int = underlyingBD.hashCode
  override def toString: String = underlyingBD.toString
  override def compare(that: Timestamp) = {
    this.underlyingBD.compare(that.underlyingBD)
  }
}

object Timestamp {
  def apply(timeBigDecimal: BigDecimal) : Timestamp = new Timestamp(timeBigDecimal)
  def apply(timeString: String) : Timestamp = new Timestamp(BigDecimal(timeString))
}

/*
// Move this to a metadata store
class SimpleESBRecordingId(val id: String)
  extends ESBRecordingId
{
  val separatorString = "_"
  val components = id.split(separatorString)
  val experiment: String = components(0)
  val subject: String = components(1)
  val blockId: String = components(2)
  def getSubject(): SubjectId = new SubjectId(subject)
  def getExperiment(): ExperimentId = new ExperimentId(experiment)
  def getBlock(): BlockId = new BlockId(blockId)
}


class UEL(recordingId: RecordingId, val timestamp: Timestamp) extends Id {
  val separatorString = ":"
  def getId() = Array(
    recordingId,
    timestamp.toString()).mkString(separatorString
  )
}

class UNL(
           val recordingId: RecordingId,
           val startTime: Timestamp,
           val endTime: Timestamp)
  extends Id
{
  val separatorString = "-"
  val identifier =
    Array(
      startTime.toString(),
      endTime.toString()).mkString(separatorString
    )
  def getId() = identifier
}
*/
