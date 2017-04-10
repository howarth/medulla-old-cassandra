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
    this.getClass == that.getClass && this.id == that.id
  }
  override def hashCode(): Int = this.id.hashCode
}

// Use if String Id becomes more complicated
// trait StringId(val id: String)

class TimeSeriesId(val id: String) extends Id {}

class SubjectId(val id: String) extends Id {}

class ExperimentId(val id: String) extends Id {}

class BlockId(val id: String) extends Id {}

// Preprocessing Readable ID
class ProcessSlugId(val id: String) extends Id {}

class StimuliSetId(val id: String) extends Id {}

class Stimulus(val id: String) {}

class RecordingChannelId(val id: String) extends Id {}

class DataId(val id: String) extends Id {}

// timeString is string of epoch
class Timestamp(val timeString: String) extends Ordered[Timestamp] {
  private val timeDecimal: BigDecimal =
    IdUtils.truncatedBigDecimal(BigDecimal(timestamp))
  val _underlyingDB: BigDecimal = t

  override def equals(that: Any): Boolean = {
    this.getClass == that.getClass && this.toString == that.toString
  }
  override def hashCode(): Int = this.timeString.hashCode
  override def toString: String = this.timeString

  override def compare(that: Timestamp) = {
    this.timeDecimal.compare(that.timeDecimal)
  }
}

object Timestamp { 
  def apply(timeString: String) = new Timestamp(timeString)
}

class Event(val timestamp: Timestamp) {}

class EventVector(events: IndexedSeq[Event])

class TriggerEvent(val triggerValue: Int, val timestamp: Timestamp)
  extends Event(timestamp)

class TriggerEventVector(events: Vector[TriggerEvent])

class StimulusEvent(stimulus: Stimulus, timestamp: Timestamp)
  extends Event(timestamp)
{}

class StimulusEventVector(val stimuliEvents: IndexedSeq[StimulusEvent]) {}

class RecordingId(val id: String) extends Id {}
  
  
  // How do traits extend when there are constructors
  // ESB - Experimental Subject block.
trait ESBRecordingId extends RecordingId(id) {
  val subjectId : ExperimentId
  val experimentId : ExperimentID
  val blockId : BlockId
  val subjectId : SubjectId
  val experimentId : ExperimentId
}
/
// / id - string seperated
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

