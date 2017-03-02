package main.scala.core
import java.nio.file.{Files, Path, Paths}
import play.api.libs.json._




trait MetadataContext {
  def getSubjects(experiment : ExperimentIdentifier) : List[SubjectIdentifier]
  def getExperiments() : List[ExperimentIdentifier]
  def getDataRecordings(experiment: ExperimentIdentifier, subject : SubjectIdentifier) : List[RecordingIdentifier]

  def getEmptyRoomRecordings(experiment: ExperimentIdentifier, subject : SubjectIdentifier) : List[RecordingIdentifier]
  def getEmptyRoomRecordingsMap(experiment: ExperimentIdentifier, subject : SubjectIdentifier) :
    Map[RecordingIdentifier, RecordingIdentifier]

  def getOtherRecordings(experiment: ExperimentIdentifier, subject : SubjectIdentifier) : List[RecordingIdentifier]
  def getStimuliSet(experiment : ExperimentIdentifier) : StimuliSetIdentifier

  def getFirstTimestamp(recording : RecordingIdentifier) : Event
  def getLastTimestamp(recording : RecordingIdentifier) : Event

  def getObservedEvents(recording : RecordingIdentifier) : EventList
  def getFixedEvents(recording : RecordingIdentifier) : EventList

  def getStimuliToIgnoreOnMatch(experiment : ExperimentIdentifier) : List[Stimulus]
  /* TODO def getResponseCodes() */
  def getZeroEventStimuli(experiment : ExperimentIdentifier) : List[Stimulus]

  def getPsychtoolboxStimuliEvents(subject : SubjectIdentifier,
                                   experiment : ExperimentIdentifier,
                                   block : BlockIdentifier) : StimulusEventList
}

class AlphaFSMetadataContext(basePathString : String) extends MetadataContext {
  val basePath: Path = Paths.get(basePathString)
  val recordingsDirPath: Path = basePath.resolve(Paths.get("recordings"))
  val experimentsPath : Path = basePath.resolve("experiments.json")
  val recordingsPath = (eid : ExperimentIdentifier) => recordingsDirPath.resolve(Array(eid.getIdentifier(), ".json").mkString(""))
  val dataRecordingsKey = "data"
  val otherRecordingsKey = "other"
  val emptyRoomRecordingsKey = "empty_room"

  private def loadJson(p: Path) : JsValue = Json.parse(Files.newInputStream(p))

  def getSubjects(experiment: ExperimentIdentifier): List[SubjectIdentifier] =
    loadJson(recordingsPath(experiment)).asOpt[Map[String, JsValue]] match {
      case Some(sub2recs : Map[String, JsValue]) => sub2recs.keys.toList.map(new SubjectIdentifier(_))
      case None => throw new Exception("The recordings file is not as expected")
    }

  def getExperiments(): List[ExperimentIdentifier] =
    loadJson(experimentsPath).asOpt[List[String]] match {
      case Some(exps : List[String]) => exps.map(new ExperimentIdentifier(_))
      case None => throw new Exception("The experiments.json file was not as expected")
    }

  def getSubjectRecordings(experiment : ExperimentIdentifier, subject : SubjectIdentifier, key : String) : JsValue =
    loadJson(recordingsPath(experiment)).asOpt[Map[String, JsValue]] match {
      case Some(sub2recs) => sub2recs(subject.getIdentifier()).asOpt[Map[String, JsValue]] match {
        case Some(recType2recs) =>
          try { recType2recs(key) } catch {case nse : NoSuchElementException => JsNull}
        case None => throw new Exception("Something went wrong when attempting to load data recordings")
      }
      case None => throw new Exception("Something went wrong when attempting to load data recordings")
    }

  def getDataRecordings(experiment: ExperimentIdentifier, subject: SubjectIdentifier): List[RecordingIdentifier] =
    this.getSubjectRecordings(experiment, subject, dataRecordingsKey).asOpt[List[String]] match {
          case Some(recStrings : List[String]) => recStrings.map( blockStr =>
            AlphaRecordingIdParser.combine(experiment)(subject)(new BlockIdentifier(blockStr)))
          case None => throw new Exception("Something went wrong when attempting to load data recordings")
        }

  def getEmptyRoomRecordingsMap(experiment: ExperimentIdentifier, subject : SubjectIdentifier) :
    Map[RecordingIdentifier, RecordingIdentifier] = {
    val ridPartial : (BlockIdentifier => RecordingIdentifier) = AlphaRecordingIdParser.combine(experiment)(subject);
    val erRecsJsValue = getSubjectRecordings(experiment, subject, emptyRoomRecordingsKey)
    erRecsJsValue.asOpt[String] match {
      case Some(erStr : String) => {
        val erRecId = ridPartial(new BlockIdentifier(erStr))
        getDataRecordings(experiment, subject).map((_, erRecId)).toMap
      }
      case None => {
        erRecsJsValue.asOpt[Map[String, List[String]]] match {
          case Some(ers : Map[String, List[String]]) => {
            ers.keys.flatMap((k: String) => {
              val erRid = ridPartial(new BlockIdentifier(k));
              ers(k).map((bidStr : String) => (ridPartial(new BlockIdentifier(bidStr)),erRid))
            }
            ).toMap
          }
          case None => throw new Exception("Something went wrong when attempting to load data recordings")
        }
      }
    }
  }

  def getEmptyRoomRecordings(experiment: ExperimentIdentifier, subject : SubjectIdentifier) : List[RecordingIdentifier] =
    getEmptyRoomRecordingsMap(experiment, subject).values.toSet.toList

  def getOtherRecordings(experiment: ExperimentIdentifier, subject : SubjectIdentifier) : List[RecordingIdentifier] =
    getSubjectRecordings(experiment, subject, otherRecordingsKey) match {
    case JsNull => Nil
    case jsv : JsValue =>
        jsv.asOpt[List[String]] match {
          case Some(recStrings: List[String]) => recStrings.map(blockStr =>
            AlphaRecordingIdParser.combine(experiment)(subject)(new BlockIdentifier(blockStr)))
          case None => throw new Exception("Something went wrong when attempting to load data recordings")
        }
    }

  def getAllRecordings() : List[RecordingIdentifier] = {
    val expsubs : List[(ExperimentIdentifier, SubjectIdentifier)]= getExperiments().flatMap(eid => getSubjects(eid).map(sid => (eid, sid)))
    expsubs.flatMap(Function.tupled((eid : ExperimentIdentifier, sid : SubjectIdentifier) =>
      getDataRecordings(eid, sid) ++ getOtherRecordings(eid,sid) ++ getEmptyRoomRecordings(eid, sid)))
  }

  def getStimuliSet(experiment : ExperimentIdentifier) : StimuliSetIdentifier =
    throw new NotImplementedError()

  def getFirstTimestamp(recording : RecordingIdentifier) : Event = throw new NotImplementedError
  def getLastTimestamp(recording : RecordingIdentifier) : Event = throw new NotImplementedError

  def getObservedEvents(recording : RecordingIdentifier) : EventList = null
  def getFixedEvents(recording : RecordingIdentifier) : EventList = null

  def getStimuliToIgnoreOnMatch(experiment : ExperimentIdentifier) : List[Stimulus] = null
  /* TODO def getResponseCodes() */
  def getZeroEventStimuli(experiment : ExperimentIdentifier) : List[Stimulus] = null

  def getPsychtoolboxStimuliEvents(subject : SubjectIdentifier,
                                   experiment : ExperimentIdentifier,
                                   block : BlockIdentifier) : StimulusEventList = null
}

trait DataContext {
  def recordingExists(recording : RecordingIdentifier, processSlug : ProcessSlugIdentifier) : Boolean
}

trait FSLocator {
  def getRecordingLocation(recording : RecordingIdentifier, processSlug : ProcessSlugIdentifier) : Path
}


object AlphaRecordingIdParser {
  val separator = "_"
  def parse(recording : RecordingIdentifier) : (ExperimentIdentifier, SubjectIdentifier, BlockIdentifier) = {
    val Array(experiment, subject, block) = recording.getIdentifier().split(separator)
    (new ExperimentIdentifier(experiment), new SubjectIdentifier(subject), new BlockIdentifier(block))
  }
  def combine(experiment : ExperimentIdentifier)(subject : SubjectIdentifier)(
              block: BlockIdentifier) : RecordingIdentifier = new RecordingIdentifier(Array(experiment, subject, block).map(id => id.getIdentifier()).mkString(separator))
}

class AlphaFSDataLocator(basePathString : String) extends FSLocator{
  val basePath = Paths.get(basePathString)
  val recordingParser = AlphaRecordingIdParser
  val dataDirString = "data"

  def getFilename(experiment : ExperimentIdentifier, subject : SubjectIdentifier, block : BlockIdentifier,
                  processSlug : ProcessSlugIdentifier) : Path =
    Paths.get(recordingParser.combine(experiment)(subject)(block).getIdentifier())

  def getExperimentDirectory(experiment : ExperimentIdentifier) : Path = basePath.resolve(experiment.getIdentifier())

  def getProcessedDirectory(experiment : ExperimentIdentifier, processSlug : ProcessSlugIdentifier) : Path =
    getExperimentDirectory(experiment).resolve(processSlug.getIdentifier())

  def getSubjectDirectory(experiment : ExperimentIdentifier, subject : SubjectIdentifier,
                          processSlug: ProcessSlugIdentifier) : Path =
    getProcessedDirectory(experiment, processSlug).resolve(subject.getIdentifier())

  def getRecordingLocation(recording : RecordingIdentifier, processSlug : ProcessSlugIdentifier) : Path = {
    val (eid : ExperimentIdentifier, sid : SubjectIdentifier, bid : BlockIdentifier) = AlphaRecordingIdParser.parse(recording)
    getSubjectDirectory(eid, sid, processSlug).resolve(getFilename(eid,sid,bid,processSlug))
  }
}

class FSDataContext(fsLocator : FSLocator) extends DataContext {
  def recordingExists(recording : RecordingIdentifier, processSlug : ProcessSlugIdentifier) : Boolean =
    Files.exists(fsLocator.getRecordingLocation(recording, processSlug))
}