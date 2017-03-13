package main.scala.core

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

trait Identifier{
  def getIdentifier(): String

  override def toString: String = getIdentifier()

  override def equals(that : Any)  : Boolean = {
    this.getClass == that.getClass && this.toString == that.toString
  }

  override def hashCode(): Int = this.toString.hashCode
}

class TimeSeriesIdentifier(timeseriesId : String) extends Identifier{
  def getIdentifier() = timeseriesId
}

class SubjectIdentifier(subjectId : String) extends Identifier{
  def getIdentifier() = subjectId
}

class ExperimentIdentifier(experimentId : String) extends Identifier{
  def getIdentifier() = experimentId
}

class BlockIdentifier(blockId : String) extends Identifier{
  def getIdentifier() = blockId
}

class ProcessSlugIdentifier(processSlugId : String) extends Identifier{
  def getIdentifier() = processSlugId
}

class StimuliSetIdentifier(stimuliSetId : String) extends Identifier{
  def getIdentifier() = stimuliSetId
}

class Stimulus(stimulus : String) {
  def getStimulus() = stimulus
}

class DataChannel(channelName : String){
  def getName() = channelName
}

class Event(timestampBD : BigDecimal) {
  val timestamp = IdUtils.truncatedBigDecimal(timestampBD)
}

class EventList(events : IndexedSeq[Event])

class TriggerEvent(triggerValue : Int, timestampBD :BigDecimal) extends Event(timestampBD)
class TriggerEventList(events : List[TriggerEvent])


class StimulusEvent(stimulus : Stimulus, timestampBD : BigDecimal) extends Event(timestampBD){
}

class StimulusEventList(stimuliEvents : IndexedSeq[StimulusEvent]){
  def getStimuliEvents() = stimuliEvents

}

class MatchedStimulusEventList

class RecordingIdentifier(recordingId : String) extends Identifier{
  val separatorString = "_"
  val components = recordingId.split(separatorString)
  val experiment : String = components(0)
  val subject : String = components(1)
  val blockId : String = components(2)
  def getIdentifier() = recordingId
  def getSubject() : String = subject
  def getExperiment() : String =  experiment
  def getBlockId() : String = blockId
}

class UEL(recordingId : RecordingIdentifier, timeBD : BigDecimal) extends Identifier {
  val separatorString = ":"
  val time = IdUtils.truncatedBigDecimal(timeBD)
  def getIdentifier() = Array(recordingId, time.toString()).mkString(separatorString)
}

class UNL(recId: RecordingIdentifier, startTimeBD : BigDecimal, endTimeBD : BigDecimal) extends Identifier{
  val separatorString = "-"
  val startTime = IdUtils.truncatedBigDecimal(startTimeBD)
  val endTime = IdUtils.truncatedBigDecimal(endTimeBD)
  val identifier = Array(startTime.toString(), endTime.toString()).mkString(separatorString)
  def getIdentifier() = identifier
}