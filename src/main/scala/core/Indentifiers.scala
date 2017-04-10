package main.scala.core
import scala.math._
/*
TODO: Why doesn't this work?
final class IdDecimal(val bd : BigDecimal, override val mc : MathContext) extends BigDecimal(bd, mc){
  def this(bd : BigDecimal) = this(bd, BigDecimal.defaultMathContext)

}
*/

/* This is likely a bad way to do this. This is my first scala file I've ever written, so back off!
TODO: figure out the scala way to do this
 */
object IdUtils{
  def truncatedBigDecimal(bd: BigDecimal) : BigDecimal = {
    BigDecimal(bd.underlying.stripTrailingZeros())
  }
}


trait Id{
  def getId(): String
  override def toString: String = getId()
  override def equals(that : Any)  : Boolean = {
    this.getClass == that.getClass && this.toString == that.toString
  }
  override def hashCode(): Int = this.toString.hashCode
}

trait DataId
trait TimeseriesId


class TimeSeriesId(timeseriesId : String) extends Id{
  def getId() = timeseriesId
}

class SubjectId(subjectId : String) extends Id{
  def getId() = subjectId
}

class ExperimentId(experimentId : String) extends Id{
  def getId() = experimentId
}

class BlockId(blockId : String) extends Id{
  def getId() = blockId
}

class ProcessSlugId(processSlugId : String) extends Id{
  def getId() = processSlugId
}

class StimuliSetId(stimuliSetId : String) extends Id{
  def getId() = stimuliSetId
}

class Stimulus(stimulus : String) {
  def getStimulus() = stimulus
}

class RecordingChannelId(channelId : String) extends Id{
  def getId = channelId
}

class Timestamp(timestamp : String) extends Ordered[Timestamp]{
  private val t : BigDecimal = IdUtils.truncatedBigDecimal(BigDecimal(timestamp))
  val _underlyingDB : BigDecimal = t

  override def equals(that : Any)  : Boolean = {
    this.getClass == that.getClass && this.toString == that.toString
  }
  override def hashCode(): Int = this.toString.hashCode
  override def toString: String = t.toString

  override def compare(that: Timestamp) = this.t.compare(that.t)
}

object Timestamp {
  def apply(timestamp : String) = new Timestamp(timestamp)
}

class Event(timestamp : Timestamp) {
  val t = timestamp
}

class EventVector(events : IndexedSeq[Event])

class TriggerEvent(triggerValue : Int, timestamp : Timestamp) extends Event(timestamp)
class TriggerEventVector(events : Vector[TriggerEvent])

class StimulusEvent(stimulus : Stimulus, timestamp : Timestamp) extends Event(timestamp){
}

class StimulusEventVector(stimuliEvents : IndexedSeq[StimulusEvent]){
  def getStimuliEvents() = stimuliEvents

}

class DataId(dataId : String) extends Id{
  def getId() = dataId
}

class RecordingId(recordingId : String) extends Id {
  override def getId() = recordingId
}

trait ESBRecordingId extends RecordingId {
  def getSubject() : SubjectId
  def getExperiment() : ExperimentId
  def getBlock() : BlockId
}

class AlphaESBRecordingId(recordingId : String) extends RecordingId(recordingId)
  with ESBRecordingId{
  val separatorString = "_"
  val components = recordingId.split(separatorString)
  val experiment : String = components(0)
  val subject : String = components(1)
  val blockId : String = components(2)
  def getSubject() : SubjectId = new SubjectId(subject)
  def getExperiment() : ExperimentId =  new ExperimentId(experiment)
  def getBlock() : BlockId = new BlockId(blockId)
}

class UEL(recordingId : RecordingId, timestamp : Timestamp) extends Id {
  val separatorString = ":"
  val t = timestamp
  def getId() = Array(recordingId, t.toString()).mkString(separatorString)
}

class UNL(val recordingId: RecordingId, val startTime : Timestamp, val endTime : Timestamp) extends Id{
  val separatorString = "-"
  val identifier = Array(startTime.toString(), endTime.toString()).mkString(separatorString)
  def getId() = identifier
}

