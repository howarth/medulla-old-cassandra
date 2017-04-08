package main.scala.core
import java.nio.ByteBuffer
import java.nio.charset.{MalformedInputException, StandardCharsets}
import java.nio.file.{Files, Path, Paths}
import java.io.{File, RandomAccessFile}
import java.nio.{ByteBuffer, DoubleBuffer, MappedByteBuffer}
import java.nio.channels.FileChannel

import scala.io.Source
import scala.util.{Failure, Success}
import play.api.libs.json._
import com.outworkers.phantom.dsl._

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

trait FileType {}
object FIFFFileType extends FileType
object BinFileType extends FileType


trait MetadataContext {
  def getAllRecordings() : Vector[RecordingId]
}

trait ESBMetadataContext extends MetadataContext{
  def getSubjects(experiment : ExperimentId) : Vector[SubjectId]
  def getExperiments() : Vector[ExperimentId]
}

trait MetadataContextWAT {
  def getSubjects(experiment : ExperimentId) : Vector[SubjectId]
  def getExperiments() : Vector[ExperimentId]
  def getDataRecordings(experiment: ExperimentId, subject : SubjectId) : Vector[RecordingId]

  def getEmptyRoomRecordings(experiment: ExperimentId, subject : SubjectId) : Vector[RecordingId]
  def getEmptyRoomRecordingsMap(experiment: ExperimentId, subject : SubjectId) :
    Map[RecordingId, RecordingId]

  def getOtherRecordings(experiment: ExperimentId, subject : SubjectId) : Vector[RecordingId]
  def getStimuliSet(experiment : ExperimentId) : StimuliSetId

  def getFirstTimestamp(recording : RecordingId) : Event
  def getLastTimestamp(recording : RecordingId) : Event

  def getObservedEvents(recording : RecordingId) : EventVector
  def getFixedEvents(recording : RecordingId) : EventVector

  def getStimuliToIgnoreOnMatch(experiment : ExperimentId) : Vector[Stimulus]
  /* TODO def getResponseCodes() */
  def getZeroEventStimuli(experiment : ExperimentId) : Vector[Stimulus]

  def getPsychtoolboxStimuliEvents(experiment : ExperimentId,
                                   subject : SubjectId,
                                   block : BlockId) : StimulusEventVector
}

class AlphaFileSystemESBMetadataContext(basePathString : String) extends ESBMetadataContext {
  val basePath: Path = Paths.get(basePathString)
  val recordingsDirPath: Path = basePath.resolve(Paths.get("recordings"))
  val experimentsPath: Path = basePath.resolve("experiments.json")
  val recordingsPath = (eid: ExperimentId) => recordingsDirPath.resolve(Array(eid.getId(), ".json").mkString(""))
  val dataRecordingsKey = "data"
  val otherRecordingsKey = "other"
  val emptyRoomRecordingsKey = "empty_room"
  val eventsDirPath: Path = basePath.resolve("events")
  val eventsExperimentDirPath: (ExperimentId => Path) = (eid: ExperimentId) => eventsDirPath.resolve(eid.getId())
  val eventsExperimentSubjectDirPath: (ExperimentId, SubjectId) => Path = (eid, sid) => {
    eventsExperimentDirPath(eid).resolve(sid.getId())
  }
  val ptbPath = (eid: ExperimentId, sid: SubjectId, bid: BlockId) => {
    eventsExperimentSubjectDirPath(eid, sid).resolve(
      Array(sid.getId(), eid.getId(), bid.getId(), "psychtoolbox.txt").mkString("_")
    )
  }

  private def loadJson(p: Path): JsValue = Json.parse(Files.newInputStream(p))

  def getSubjects(experiment: ExperimentId): Vector[SubjectId] =
    loadJson(recordingsPath(experiment)).asOpt[Map[String, JsValue]] match {
      case Some(sub2recs: Map[String, JsValue]) => sub2recs.keys.toVector.map(new SubjectId(_))
      case None => throw new Exception("The recordings file is not as expected")
    }

  def getExperiments(): Vector[ExperimentId] =
    loadJson(experimentsPath).asOpt[Vector[String]] match {
      case Some(exps: Vector[String]) => exps.map(new ExperimentId(_))
      case None => throw new Exception("The experiments.json file was not as expected")
    }

  def getSubjectRecordings(experiment: ExperimentId, subject: SubjectId, key: String): JsValue =
    loadJson(recordingsPath(experiment)).asOpt[Map[String, JsValue]] match {
      case Some(sub2recs) => sub2recs(subject.getId()).asOpt[Map[String, JsValue]] match {
        case Some(recType2recs) =>
          try {
            recType2recs(key)
          } catch {
            case nse: NoSuchElementException => JsNull
          }
        case None => throw new Exception("Something went wrong when attempting to load data recordings")
      }
      case None => throw new Exception("Something went wrong when attempting to load data recordings")
    }

  def getDataRecordings(experiment: ExperimentId, subject: SubjectId): Vector[RecordingId] =
    this.getSubjectRecordings(experiment, subject, dataRecordingsKey).asOpt[Vector[String]] match {
      case Some(recStrings: Vector[String]) => recStrings.map(blockStr =>
        AlphaRecordingIdParser.combine(experiment)(subject)(new BlockId(blockStr)))
      case None => throw new Exception("Something went wrong when attempting to load data recordings")
    }

  def getEmptyRoomRecordingsMap(experiment: ExperimentId, subject: SubjectId):
  Map[RecordingId, RecordingId] = {
    val ridPartial: (BlockId => RecordingId) = AlphaRecordingIdParser.combine(experiment)(subject);
    val erRecsJsValue = getSubjectRecordings(experiment, subject, emptyRoomRecordingsKey)
    erRecsJsValue.asOpt[String] match {
      case Some(erStr: String) => {
        val erRecId = ridPartial(new BlockId(erStr))
        getDataRecordings(experiment, subject).map((_, erRecId)).toMap
      }
      case None => {
        erRecsJsValue.asOpt[Map[String, Vector[String]]] match {
          case Some(ers: Map[String, Vector[String]]) => {
            ers.keys.flatMap((k: String) => {
              val erRid = ridPartial(new BlockId(k));
              ers(k).map((bidStr: String) => (ridPartial(new BlockId(bidStr)), erRid))
            }
            ).toMap
          }
          case None => throw new Exception("Something went wrong when attempting to load data recordings")
        }
      }
    }
  }

  def getEmptyRoomRecordings(experiment: ExperimentId, subject: SubjectId): Vector[RecordingId] =
    getEmptyRoomRecordingsMap(experiment, subject).values.toSet.toVector

  def getOtherRecordings(experiment: ExperimentId, subject: SubjectId): Vector[RecordingId] =
    getSubjectRecordings(experiment, subject, otherRecordingsKey) match {
      case JsNull => Vector()
      case jsv: JsValue =>
        jsv.asOpt[Vector[String]] match {
          case Some(recStrings: Vector[String]) => recStrings.map(blockStr =>
            AlphaRecordingIdParser.combine(experiment)(subject)(new BlockId(blockStr)))
          case None => throw new Exception("Something went wrong when attempting to load data recordings")
        }
    }

  def getAllRecordings(): Vector[RecordingId] = {
    val expsubs: Vector[(ExperimentId, SubjectId)] = getExperiments().flatMap(eid => getSubjects(eid).map(sid => (eid, sid)))
    expsubs.flatMap(Function.tupled((eid: ExperimentId, sid: SubjectId) =>
      getDataRecordings(eid, sid) ++ getOtherRecordings(eid, sid) ++ getEmptyRoomRecordings(eid, sid)))
  }

  def getStimuliSet(experiment: ExperimentId): StimuliSetId =
    throw new NotImplementedError()

  def getFirstTimestamp(recording: RecordingId): Event = throw new NotImplementedError

  def getLastTimestamp(recording: RecordingId): Event = throw new NotImplementedError

  def getObservedEvents(recording: RecordingId): EventVector = null

  def getFixedEvents(recording: RecordingId): EventVector = null

  def getStimuliToIgnoreOnMatch(experiment: ExperimentId): Vector[Stimulus] = null

  /* TODO def getResponseCodes() */
  def getZeroEventStimuli(experiment: ExperimentId): Vector[Stimulus] = null

  val _orderedCharsets = Array(StandardCharsets.US_ASCII, StandardCharsets.ISO_8859_1)

  def getPsychtoolboxStimuliEvents(experiment: ExperimentId,
                                   subject: SubjectId,
                                   block: BlockId): StimulusEventVector = {
    val ptbFilePath = ptbPath(experiment, subject, block)
    for (cs <- _orderedCharsets) {
      try {
        val source = Source.fromFile(ptbFilePath.toString(), cs.toString())
        val ptbContent: String = source.getLines mkString "\n"
        val elements = ptbContent.split("\t").zipWithIndex.flatMap {
          case (e, i) => (i % 4) match {
            case 0 => {
              e.split("\n")
            }
            case _ => Array(e)
          }
        }

        elements.length % 5 match {
          case 0 => None
          case _ => throw new IllegalArgumentException
        }
        return new StimulusEventVector(
          Range(0, elements.length / 5).map(ei =>
            new StimulusEvent(new Stimulus(elements((ei * 5) + 1).trim),
              Timestamp(elements((ei * 5) + 3).trim)))
        )

      } catch {
        case _: MalformedInputException => {}
      }
    }
    throw new MalformedInputException(1)
  }
}


trait FSDataLocator {
  def getRecordingLocation(recording : RecordingId, processSlug : ProcessSlugId) : Path
  def getRecordingChannelLocation(recording : RecordingId, processSlug : ProcessSlugId,
                                  channel : RecordingChannelId) : Path
}

object AlphaRecordingIdParser {
  val separator = "_"
  def parse(recording : RecordingId) : (ExperimentId, SubjectId, BlockId) = {
    val Array(experiment, subject, block) = recording.getId().split(separator)
    (new ExperimentId(experiment), new SubjectId(subject), new BlockId(block))
  }
  def combine(experiment : ExperimentId)(subject : SubjectId)(
              block: BlockId) : RecordingId = new RecordingId(Array(experiment, subject, block).map(id => id.getId()).mkString(separator))
}

class AlphaFIFFSDataLocator(basePathString : String) extends FSDataLocator{
  val basePath = Paths.get(basePathString)
  val recordingParser = AlphaRecordingIdParser
  val dataDirString = "data"

  def getFilename(experiment : ExperimentId, subject : SubjectId, block : BlockId,
                  processSlug : ProcessSlugId) : String = {
    val ending = processSlug.getId() match {
      case "raw" => "raw.fif"
      case ps : String => Array(ps, "raw.fif").mkString("_")
    }
    Array(subject.getId(), experiment.getId(), block.getId(), ending).mkString("_")
  }

  def getExperimentDirectory(experiment : ExperimentId) : Path = basePath.resolve(experiment.getId())

  def getProcessedDirectory(experiment : ExperimentId, processSlug : ProcessSlugId) : Path =
    getExperimentDirectory(experiment).resolve(dataDirString).resolve(processSlug.getId())

  def getSubjectDirectory(experiment : ExperimentId, subject : SubjectId,
                          processSlug: ProcessSlugId) : Path =
    getProcessedDirectory(experiment, processSlug).resolve(subject.getId())

  def getRecordingLocation(recording : RecordingId, processSlug : ProcessSlugId) : Path = {
    val (eid : ExperimentId, sid : SubjectId, bid : BlockId) = AlphaRecordingIdParser.parse(recording)
    getSubjectDirectory(eid, sid, processSlug).resolve(getFilename(eid,sid,bid,processSlug))
  }

  def getRecordingChannelLocation(recording : RecordingId, processSlug : ProcessSlugId,
                                  channel : RecordingChannelId) : Path = throw new Exception("All channels are stored together")
}

trait BinFSDataLocator extends FSDataLocator

class AlphaBinDataLocator(basePathString : String)  extends AlphaFIFFSDataLocator(basePathString) with BinFSDataLocator{
  override val dataDirString = "bin"

  def getFilename(experiment : ExperimentId, subject : SubjectId, block : BlockId,
                           processSlug : ProcessSlugId, channel: RecordingChannelId) : String = {
    val ending = Array(processSlug.getId(), ".bin").mkString
    Array(subject.getId(), experiment.getId(), block.getId(), channel.getId, ending).mkString("_")
  }

  override def getRecordingLocation(recording : RecordingId, processSlug : ProcessSlugId) : Path =
    throw new Exception("Channels are stored separately")

  override def getRecordingChannelLocation(recording : RecordingId, processSlug : ProcessSlugId,
                                  channel : RecordingChannelId) : Path = {
    val (eid : ExperimentId, sid : SubjectId, bid : BlockId) = AlphaRecordingIdParser.parse(recording)
    getSubjectDirectory(eid, sid, processSlug).resolve(bid.getId()).resolve(getFilename(eid,sid,bid,processSlug, channel))
  }
}

trait RecordingDataContext{
  def recordingExists(recording : RecordingId, processSlug : ProcessSlugId) : Boolean
}

trait ReadableRecordingDataContext extends RecordingDataContext {
  def getStartTime(recording : RecordingId, processSlug : ProcessSlugId) : Timestamp
  def getEndTime(recording : RecordingId, processSlug : ProcessSlugId) : Timestamp
  def getChannels(recording : RecordingId, processSlug : ProcessSlugId) : Vector[RecordingChannelId]
  def getChannelData(recording : RecordingId, processSlug : ProcessSlugId, channel : RecordingChannelId): SingleChannelTimeseries
  def getDataFromUNL(unl : UNL, processSlug : ProcessSlugId) : MultiChannelTimeseries
  def getChannelDataFromUNL(unl : UNL, channel : RecordingChannelId) : SingleChannelTimeseries
  def getMultiChannelDataFromUNL( unl : UNL, channels : Vector[RecordingChannelId]) : MultiChannelTimeseries
  def getTimes(recording : RecordingId, processSlug : ProcessSlugId) : Vector[Timestamp]
  /*
  def getVector(vectorId : String) : Vector[Double]
  def get2DMatrix(matrixId : String) : Vector[Vector[Double]]
  */
}
trait WritableRecordingDataContext extends RecordingDataContext {
  def putData( recording : RecordingId, processSlug : ProcessSlugId, data : MultiChannelTimeseries) : Unit
  def putChannelData( recording : RecordingId, processSlug : ProcessSlugId, data : SingleChannelTimeseries) : Unit
  /*
  def putVector(vectorId : String, vec : Vector[Double]) : Unit
  def put2DMatrix(matrixId : String, mat : Vector[Vector[Double]]) : Unit
  */
}

abstract class FSDataContext(fsLocator : FSDataLocator) extends ReadableRecordingDataContext {
  def recordingExists(recording : RecordingId, processSlug : ProcessSlugId) : Boolean =
    Files.exists(fsLocator.getRecordingLocation(recording, processSlug))
}

class FIFDataContext(fsLocator : FSDataLocator) extends FSDataContext(fsLocator) {
  val timeChan = new RecordingChannelId("time")
  def getStartTime(recording : RecordingId, processSlug : ProcessSlugId) : Timestamp = throw new Exception("Doesn't exist for FIF yet")
  def getEndTime(recording : RecordingId, processSlug : ProcessSlugId) : Timestamp = throw new Exception("Doesn't exist for FIF yet")
  def getChannels(recording : RecordingId, processSlug : ProcessSlugId) : Vector[RecordingChannelId] = throw new Exception("Doesn't exist for FIF yet")
  def getChannelData(recording: RecordingId, processSlug: ProcessSlugId, channel : RecordingChannelId) :SingleChannelTimeseries = throw new Exception("no fif")
  def getDataFromUNL(unl : UNL, processSlug : ProcessSlugId) : MultiChannelTimeseries = throw new Exception("Doesn't exist for FIF yet")
  def getChannelDataFromUNL( unl : UNL, channel : RecordingChannelId) : SingleChannelTimeseries = throw new Exception("Doesn't exist for FIF yet")
  def getMultiChannelDataFromUNL(unl : UNL, channels : Vector[RecordingChannelId]) : MultiChannelTimeseries = throw new Exception("Doesn't exist for FIF yet")
  def getTimes(recording : RecordingId, processSlug : ProcessSlugId) : Vector[Timestamp] = throw new Exception("Doesn't exist for FIF yet")
}

class BinDataContext(fsLocator : BinFSDataLocator) extends FSDataContext(fsLocator) {
  val timeChan = new RecordingChannelId("time")
  private val channelsChan = new RecordingChannelId("channels")

  def readFullChannel(recording : RecordingId, processSlug : ProcessSlugId)(channel : RecordingChannelId) : Vector[Double] = {
    val headerLength = 1
    val numBytesPerNum = 8
    val channelFile : File = fsLocator.getRecordingChannelLocation(recording, processSlug, channel).toFile
    val mbb : MappedByteBuffer= new RandomAccessFile(channelFile, "r").getChannel.map(FileChannel.MapMode.READ_ONLY, 0, channelFile.length())
    mbb.getDouble match {
      case 1.0 => {
        Vector.tabulate[Double](mbb.remaining()/numBytesPerNum)(index => mbb.getDouble(numBytesPerNum * (headerLength + index)))
      }
      case x => throw new Exception(s"uh oh $x")
    }
  }

  private def getTimeFileIter(recording : RecordingId, processSlug: ProcessSlugId) : Iterator[String] = {
    val timePath: Path = fsLocator.getRecordingChannelLocation(recording, processSlug, timeChan)
    Source.fromFile(timePath.toFile).getLines
  }

  def getStartTime(recording : RecordingId, processSlug : ProcessSlugId) : Timestamp = {
    val iter = getTimeFileIter(recording, processSlug)
    Timestamp(iter.next())
  }

  def getEndTime(recording : RecordingId, processSlug : ProcessSlugId) : Timestamp = {
    val iter = getTimeFileIter(recording, processSlug)
    iter.next()
    Timestamp(iter.next())
  }

  def getChannels(recording : RecordingId, processSlug : ProcessSlugId) : Vector[RecordingChannelId] = {
    val channelPath : Path = fsLocator.getRecordingChannelLocation(recording,processSlug, channelsChan)
    Source.fromFile(channelPath.toFile).getLines.map(new RecordingChannelId(_)).toVector
  }

  override def getChannelData(recording: RecordingId, processSlug: ProcessSlugId, channel : RecordingChannelId) :SingleChannelTimeseries =
    new SingleChannelTimeseries(readFullChannel(recording,processSlug)(channel),getTimes(recording, processSlug), channel)
  def getAllData(recording : RecordingId, processSlug : ProcessSlugId) : MultiChannelTimeseries =throw new Exception("Doesn't exist for bin yet")
  /*{
    val chans = getChannels(recording, processSlug).slice(0,200)
    val rfc = readFullChannel(recording,processSlug)(_)
    val data : Vector[Vector[Double]] = Vector.tabulate[Vector[Double]](chans.length)(i => rfc(chans(i)))
    val times : Vector[Timestamp] = getTimes(recording, processSlug)
    println("done this part")
    new MultiChannelTimeseries(data, times, chans)
  }
  */
  def getDataFromUNL(unl : UNL, processSlug : ProcessSlugId) : MultiChannelTimeseries = throw new Exception("Doesn't exist for bin yet")
  def getChannelData(recording : RecordingChannelId, channel : RecordingChannelId) : SingleChannelTimeseries = throw new Exception("Doesn't exist for bin yet")
  def getChannelDataFromUNL(unl : UNL, channel : RecordingChannelId) : SingleChannelTimeseries = throw new Exception("Doesn't exist for bin yet")
  def getMultiChannelDataFromUNL(unl : UNL, channels : Vector[RecordingChannelId]) : MultiChannelTimeseries = throw new Exception("Doesn't exist for bin yet")
  def getTimes(recording : RecordingId, processSlug : ProcessSlugId) : Vector[Timestamp] = {
    val iter = getTimeFileIter(recording, processSlug)
    iter.next()
    iter.next()
    iter.map(Timestamp(_)).toVector
  }
}

case class ChannelsRecord(recording_id  : String, channel_names: List[String])
class ChannelsTable extends CassandraTable[ChannelsTable, ChannelsRecord] {
  override val tableName = "channels"
  object recording_id extends StringColumn(this) with PartitionKey
  object channel_names extends ListColumn[String](this)
}

case class RecordingRecord(recording_id : String, process_slug : String)
class RecordingsTable extends CassandraTable[RecordingsTable, RecordingRecord] {
  override val tableName = "recordings"
  object recording_id extends StringColumn(this) with PartitionKey
  object process_slug extends StringColumn(this) with PrimaryKey
}
/*
create table if not exists recording_data(
  recording_id text,
  timestamp decimal,
  process_slug text,
  channel_name text,
  value double,
  PRIMARY KEY (recording_id, process_slug, channel_name, timestamp) ,
 ) WITH CLUSTERING ORDER BY (process_slug asc, channel_name asc, timestamp asc);
*/

case class RecordingDataRecord(
  recording_id : String,
  timestamp : BigDecimal,
  process_slug : String,
  channel_name : String,
  value: Double)

class RecordingDataTable extends CassandraTable[RecordingDataTable, RecordingDataRecord] {
  override val tableName = "recording_data"
  object recording_id extends StringColumn(this) with PartitionKey
  object timestamp extends BigDecimalColumn(this) with PrimaryKey with ClusteringOrder with Ascending
  object process_slug extends StringColumn(this) with PrimaryKey with ClusteringOrder with Ascending
  object channel_name extends StringColumn(this) with PrimaryKey with ClusteringOrder with Ascending
  object value extends DoubleColumn(this)
}

/*
create table if not exists time_bounds(
recording_id text,
start_time decimal,
end_time decimal,
PRIMARY KEY(recording_id));
*/

case class TimeBoundsRecord(recording_id : String, start_time : BigDecimal, end_time : BigDecimal)
class TimeBoundsTable extends CassandraTable[TimeBoundsTable, TimeBoundsRecord]{
  override val tableName = "time_bounds"
  object recording_id extends StringColumn(this) with PartitionKey
  object start_time extends BigDecimalColumn(this)
  object end_time extends BigDecimalColumn(this)

}

case class TimestampsRecord(recording_id : String, times : List[BigDecimal])
class TimestampsTable extends CassandraTable[TimestampsTable, TimestampsRecord] {
  override val tableName = "timestamps"
  object recording_id extends StringColumn(this) with PartitionKey
  object times extends ListColumn[BigDecimal](this)
}

case class VectorData(vector_id : String, vector_data: List[Double])
class VectorTable extends CassandraTable[VectorTable, VectorData]{
  override val tableName = "vector_data"
  object vector_id extends StringColumn(this) with PartitionKey
  object vector_data extends ListColumn[Double](this)
}

case class Matrix2DData(matrix_id : String, matrix_data : List[List[Double]])
class Matrix2DTable extends CassandraTable[Matrix2DTable, Matrix2DData]{
  override val tableName = "matrix2D_data"
  object matrix_id extends StringColumn(this) with PartitionKey
  object matrix_data extends ListColumn[List[Double]](this)
}

class CassPhantomDatabase(override val connector : KeySpaceDef) extends Database[CassPhantomDatabase](connector) {

  object timeBoundsT extends TimeBoundsTable with connector.Connector
  object timestampsT extends TimestampsTable with connector.Connector
  object recordingsT extends RecordingsTable with connector.Connector
  object channelsT extends ChannelsTable with connector.Connector
  object recordingDataT extends RecordingDataTable with connector.Connector
  object vectorDataT extends VectorTable with connector.Connector
  object matrix2DDataT extends Matrix2DTable with connector.Connector

  def recordingExists(recordingId : String, processSlug : String) : Boolean = {
    val future = recordingsT.select.where(_.recording_id eqs recordingId).and(_.process_slug eqs processSlug).future()
    val resultRows = Await.result(future, 10 seconds).all()
    (resultRows.size > 0)
  }

  def getTimeBounds(recordingId : String, processSlug : String) : Tuple2[Timestamp, Timestamp] = {
    val future = timeBoundsT.select.where(_.recording_id eqs recordingId).future()
    val resultRows  = Await.result(future, 10 seconds).all()
    if (resultRows.size() > 1) {
      throw new Exception("Expected one time bound for recording id")
    }
    if (resultRows.size() == 0) {
      throw new Exception("Expected one time bound for recording id, not found")
    }
    val tb = timeBoundsT.fromRow(resultRows.get(0))
    Tuple2(Timestamp(tb.start_time.toString), Timestamp(tb.end_time.toString))
  }

  def putTimes(recordingId : String, times : Vector[Timestamp]) : Unit =  {
    timestampsT.insert().value(_.recording_id, recordingId).value(_.times, times.map(_._underlyingDB).toList)
      .future().onComplete {
      case Success(e) => ()
      case Failure(e) => throw e
    }
  }

  def getTimes(recordingId : String) : Vector[Timestamp] = {
    val resultRows = Await.result(timestampsT.select.where(_.recording_id eqs recordingId).future(), 10 seconds).all()
    if (resultRows.size() > 1) {
      throw new Exception("Expected one time list for recording id")
    }
    if (resultRows.size() == 0) {
      throw new Exception("Expected one time list for recording id, not found")
    }
    timestampsT.fromRow(resultRows.get(0)).times.map(t => new Timestamp(t.toString)).toVector
  }

  def putTimeBounds(recordingId : String, processSlug : String,
                    startTime : Timestamp, endTime : Timestamp): Unit = {
    timeBoundsT.insert
        .value(_.recording_id, Array(recordingId.toString, processSlug.toString).mkString)
        .value(_.start_time, startTime._underlyingDB)
            .value(_.end_time, endTime._underlyingDB).future().onComplete {
      case Success(e) => ()
      case Failure(e) => throw e
    }
  }

  def putChannels(recordingId : String, processSlug : String, channels : List[String]) : Unit = {
    channelsT.insert.value(_.recording_id, Array(recordingId,processSlug).mkString)
      .value(_.channel_names, channels).future().onComplete {
      case Success(e) => ()
      case Failure(e) => throw e
    }
  }

  def getChannels(recordingId : String, processSlug : String) : List[String] ={
    val future = channelsT.select.where(_.recording_id eqs Array(recordingId,processSlug).mkString).future()
    val resultRows  = Await.result(future, 10 seconds).all()
    if (resultRows.size() > 1) {
      throw new Exception("Expected one channels for recording id")
    }
    if (resultRows.size() == 0) {
      throw new Exception("Expected one channels for recording id, not found")
    }
    channelsT.fromRow(resultRows.get(0)).channel_names

  }

  def putChannelData(recordingId : String, processSlug : String, channel : String, times : Vector[BigDecimal], data: Vector[Double]) = {
    val ins = recordingDataT.insert().value(_.recording_id, recordingId).value(_.process_slug, processSlug).value(_.channel_name, channel)
    val futures : Vector[Future[ResultSet]] = data.zip(times).map( z => z match {case (d : Double,t : BigDecimal) =>
        ins.value(_.timestamp, t).value(_.value, d).future()
    })
  }

  def getChannelData(recordingId : String, processSlug : String, channel : String, startTime : BigDecimal, endTime : BigDecimal) : Vector[Double] = {
    val future = recordingDataT.select
      .where(_.process_slug eqs processSlug).and(_.recording_id eqs recordingId)
      .and(_.channel_name eqs channel).and(_.timestamp gte startTime).and(_.timestamp lte endTime).orderBy(t => t.timestamp asc).future()
    val resultRows = Await.result(future, 10 seconds).all()
    Vector.tabulate[Double](resultRows.size()){ri => recordingDataT.fromRow(resultRows.get(ri)).value}
  }

  def getVector(vecId: String) : Vector[Double]= {
    val future = vectorDataT.select.where(_.vector_id eqs vecId).future()
    val resultRows = Await.result(future, 10 seconds).all()
    if (resultRows.size() > 1) {
        throw new Exception(s"Expected only one vector per vector id : $vecId")
      }
    if (resultRows.size() == 0) {
      throw new Exception(s"Expected a vector for vector id $vecId")
    }
    vectorDataT.fromRow(resultRows.get(0)).vector_data.toVector
  }

  def putVector(vecId : String, vec : Vector[Double]) = {
    val ins = vectorDataT.insert().value(_.vector_id, vecId).value(_.vector_data, vec.toList).future().onComplete {
      case Success(e) => ()
      case Failure(e) => throw e
    }
  }

  def getMatrix2D(matrixId: String) = { //: Vector[Vector[Double]]= {
    val future = matrix2DDataT.select.where(_.matrix_id eqs matrixId).future()
    val resultRows = Await.result(future, 10 seconds).all()
    if (resultRows.size() > 1) {
      throw new Exception(s"Expected only one matrix per matrix id : $matrixId")
    }
    if (resultRows.size() == 0) {
      throw new Exception(s"Expected a matrix for matrix id $matrixId")
    }
    resultRows
    //matrix2DDataT.fromRow(resultRows.get(0)).matrix_data.map(_.toVector).toVector
  }

  def putMatrix2D(matrixId : String, matrix : Vector[Vector[Double]]) = {
    val ins = matrix2DDataT.insert()
      .value(_.matrix_id, matrixId)
      .value(_.matrix_data, matrix.toList.map(_.toList)).future().onComplete {
      case Success(e) => ()
      case Failure(e) => throw e
    }
  }

}


class CassandraRecordingDataContext(hosts : Seq[String], keySpace: String) extends
  ReadableRecordingDataContext with
  WritableRecordingDataContext {

  val connector : KeySpaceDef = ContactPoint.local.noHeartbeat().keySpace(keySpace)
  val db = new CassPhantomDatabase(connector)


  def recordingExists(recording: RecordingId, processSlug: ProcessSlugId) : Boolean =
    db.recordingExists(recording.toString, processSlug.toString)

  private def putTimeBounds(recording : RecordingId, processSlug : ProcessSlugId, startTime : Timestamp, endTime: Timestamp) : Unit =
    db.putTimeBounds(recording.toString, processSlug.toString, startTime, endTime)

  def getStartTime(recording : RecordingId, processSlug : ProcessSlugId) : Timestamp =
    db.getTimeBounds(recording.toString, processSlug.toString)._1

  def getEndTime(recording : RecordingId, processSlug : ProcessSlugId) : Timestamp =
    db.getTimeBounds(recording.toString, processSlug.toString)._2

  def getChannels(recording : RecordingId, processSlug : ProcessSlugId) : Vector[RecordingChannelId] =
    db.getChannels(recording.toString, processSlug.toString).map(new RecordingChannelId(_)).toVector

  private def putChannels(recording : RecordingId, processSlug : ProcessSlugId, channels : Vector[RecordingChannelId]) : Unit =
    db.putChannels(recording.toString, processSlug.toString, channels.map(_.toString).toList)

  def getChannelData(recording : RecordingId, processSlug : ProcessSlugId, channel : RecordingChannelId) : SingleChannelTimeseries = {
    val (start_t, end_t) : (Timestamp, Timestamp) = db.getTimeBounds(recording.toString, processSlug.toString)
    val unl = new UNL(recording, start_t, end_t)
    val data : Vector[Double] = db.getChannelData(recording.getId,processSlug.getId,channel.getId,start_t._underlyingDB, end_t._underlyingDB)
    val times : Vector[Timestamp] = getTimes(recording, processSlug)
    new SingleChannelTimeseries(data, times, channel)
  }

  def getDataFromUNL(unl : UNL, processSlug : ProcessSlugId) : MultiChannelTimeseries = {
    val chans = getChannels(unl.recordingId, processSlug);
    val data = Vector.tabulate[Vector[Double]](chans.length)(ci =>
      db.getChannelData(unl.recordingId.getId,processSlug.getId,chans(ci).getId,unl.startTime._underlyingDB, unl.endTime._underlyingDB)
    )
    val times = getTimes(unl.recordingId, processSlug)
    new MultiChannelTimeseries(data, times, chans)
  }

  def getChannelDataFromUNL(unl : UNL, channel : RecordingChannelId) : SingleChannelTimeseries = throw new NotImplementedError()

  def getMultiChannelDataFromUNL( unl : UNL, channels : Vector[RecordingChannelId]) : MultiChannelTimeseries = throw new NotImplementedError()

  def getTimes(recording : RecordingId, processSlug : ProcessSlugId) : Vector[Timestamp] = {
    db.getTimes(recording.getId())
  }

  def putChannelData( recording : RecordingId, processSlug : ProcessSlugId, data : SingleChannelTimeseries) : Unit = {
    db.putChannelData(recording.toString , processSlug.toString, data.channel.toString, data.time.map(_._underlyingDB), data.data)
  }

  def putData( recording : RecordingId, processSlug : ProcessSlugId, data : MultiChannelTimeseries) : Unit = throw new NotImplementedError()

  /*
  def getVector(vecId : String) : Vector[Double]) =
    db.getVector(vecId)

  def putVector(vecId : String, vec : Vector[Double])
    db.putVector(vecId , vec)
*/
}
