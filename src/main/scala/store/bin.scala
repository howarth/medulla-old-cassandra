package main.scala.store
import main.scala.core._

/**
  * Created by dhowarth on 5/1/17.
  */
class BinStore(basePath : String) extends
  ScalarRWDataStore[Double] with
  VectorRWDataStore[Double] with
  Matrix2DRWDataStore[Double] with
  SingleChannelTimeSeriesRWDataStore[Double] with
  MultiChannelTimeSeriesRWDataStore[Double] {

  override def putScalar(id: ScalarId, data: ScalarData[Double]) = throw new NotImplementedError()
  override def getScalar(id: ScalarId) : ScalarData[Double] = throw new NotImplementedError()
  override def deleteScalar(id: ScalarId) = throw new NotImplementedError()

  override def putVector(id: VectorId, data: VectorData[Double]) = throw new NotImplementedError()
  override def getVector(id: VectorId) = throw new NotImplementedError()
  override def deleteVector(id: VectorId) = throw new NotImplementedError()

  override def getMatrix2D(id: Matrix2DId) = throw new NotImplementedError()
  override def putMatrix2D(id: Matrix2DId, data: Matrix2DData[Double]) = throw new NotImplementedError()
  override def deleteMatrix2D(id: Matrix2DId) = throw new NotImplementedError()

  override def getSingleChannelTimeSeries(id: SingleChannelTimeSeriesId) : SingleChannelTimeSeriesData[Double] = {
    /*
    val chan = db.getChannels(id.id)
    if(chan.length != 1) throw new Error
    val bounds = db.getTimeSeriesTimeBounds(id.id)
    val times : Vector[Timestamp] = db.getTimes(id.id).map(Timestamp(_))
    val data : Vector[Double] = db.getChannelData(id.id, chan(0), bounds._1, bounds._2)
    DoubleSingleChannelTimeSeriesData(data, times, TimeSeriesChannelId(chan(0)))
    */
  }

  override def putSingleChannelTimeSeries(id: SingleChannelTimeSeriesId,
                                          data : SingleChannelTimeSeriesData[Double]) = throw new NotImplementedError()

  override def deleteSingleChannelTimeSeries(id: SingleChannelTimeSeriesId) = throw new NotImplementedError()

  def dataExists(id: main.scala.core.DataId): Unit = throw new NotImplementedError
  def getChannel(id: main.scala.core.SingleChannelTimeSeriesId): main.scala.core.TimeSeriesChannelId = throw new NotImplementedError
  def getFirstTimestamp(id: main.scala.core.TimeSeriesId): main.scala.core.Timestamp =  throw new NotImplementedError
  def getLastTimestamp(id: main.scala.core.TimeSeriesId): main.scala.core.Timestamp =  throw new NotImplementedError

  private def getTimeFileIter(recording : RecordingId, processSlug: ProcessSlugId) : Iterator[String] = {
    val timePath: Path = fsLocator.getRecordingChannelLocation(recording, processSlug, timeChan)
    Source.fromFile(timePath.toFile).getLines
  }

  def getFirstTimestamp(id: main.scala.core.TimeSeriesId): main.scala.core.Timestamp = {
    val iter = getTimeFileIter(recording, processSlug)
    Timestamp(iter.next())
  }

  def getLastTimestamp(id : TimeSeriesId) : main.scala.core.Timestamp = {
    val iter = getTimeFileIter(recording, processSlug)
    iter.next()
    Timestamp(iter.next())
  }

  def getTimes(id: main.scala.core.TimeSeriesId): Vector[main.scala.core.Timestamp] = {
    val iter = getTimeFileIter(recording, processSlug)
    iter.next()
    iter.next()
    iter.map(Timestamp(_)).toVector
  }

  def getChannels(id: main.scala.core.MultiChannelTimeSeriesId): Vector[main.scala.core.TimeSeriesChannelId] = {

  }

  override def getMultiChannelTimeSeries(id: MultiChannelTimeSeriesId) = throw new NotImplementedError()
  override def putMultiChannelTimeSeries(id: MultiChannelTimeSeriesId, data: MultiChannelTimeSeriesData[Double]) = throw new NotImplementedError()
  override def deleteMultiChannelTimeSeries(id: MultiChannelTimeSeriesId) = throw new NotImplementedError()
}
