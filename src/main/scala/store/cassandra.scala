package main.scala.store
import main.scala.core
import main.scala.core._
import com.outworkers.phantom.dsl._
import scala.util.{Failure, Success}
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
//import main.scala.store.{Matrix2DRWDataStore, VectorRWDataStore}

context (CassajdraStore, foostore)

class CassandraStore(hosts : Seq[String], keySpaceString : String) extends
  VectorRWDataStore[Double] with
  Matrix2DRWDataStore[Double] with
  SingleChannelTimeSeriesRWDataStore[Double] with
  MultiChannelTimeSeriesRWDataStore[Double] {

  val connector : KeySpaceDef = ContactPoint.local.noHeartbeat().keySpace(keySpaceString)
  val db = new CassPhantomDatabase(connector)

  override def putVector[Double](id: VectorId, data: VectorData[Double]) = db.putVector[Double](id.id, data.data)
  override def getVector[Double](id: VectorId) = new VectorData[Double](db.getVector[Double](id.id))

  override def getMatrix2D[Double](id: Matrix2DId) = db.getMatrix2D[Double](id)
  override def putMatrix2D[Double](id: Matrix2DId, data: Matrix2DData[Double]) = db.putMatrix2D[Double](id, data.data)

  override def getSingleChannelTimeSeries[Double](id: SingleChannelTimeSeriesId) = {
    //db.getgetTimeBounds
  }
  override def putSingleChannelTimeSeries[Double](id: SingleChannelTimeSeriesId, data: SingleChannelTimeSeriesData[Double]) = ???
}


case class TimeSeriesChannelsRecord(id  : String, channel_names: List[String])
class ChannelsTable extends CassandraTable[ChannelsTable, ChannelsRecord] {
  override val tableName = "timeseries_channels"
  object id extends StringColumn(this) with PartitionKey
  object channel_names extends ListColumn[String](this)
}

case class TimeseriesIdRecord(id : String)
class RecordingsTable extends CassandraTable[RecordingsTable, RecordingRecord] {
  override val tableName = "timeseries_id"
  object id extends StringColumn(this) with PartitionKey
}

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

case class DoubleVectorDataRow(vector_id : String, vector_data: List[Double])
class DoubleVectorTable extends CassandraTable[VectorTable, VectorDataRow]{
  override val tableName = "vector_data"
  object vector_id extends StringColumn(this) with PartitionKey
  object vector_data extends ListColumn[Double](this)
}

case class DoubleMatrix2DDataRow(matrix_id : String, matrix_data : List[List[Double]])
class DoubleMatrix2DTable extends CassandraTable[Matrix2DTable, Matrix2DDataRow]{
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
  object doubleVectorDataT extends DoubleVectorTable with connector.Connector
  object doubleMatrix2DDataT extends DoubleMatrix2DTable with connector.Connector

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
    timestampsT.insert().value(_.recording_id, recordingId).value(_.times, times.map(_.underlyingBD).toList)
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
    timestampsT.fromRow(resultRows.get(0)).times.map(t => Timestamp(t.toString)).toVector
  }

  def putTimeBounds(recordingId : String, processSlug : String,
                    startTime : Timestamp, endTime : Timestamp): Unit = {
    timeBoundsT.insert
      .value(_.recording_id, Array(recordingId.toString, processSlug.toString).mkString)
      .value(_.start_time, startTime.underlyingBD)
      .value(_.end_time, endTime.underlyingBD).future().onComplete {
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

  def getVector[Double](vecId: String) : Vector[Double]= {
    val future = DoubleVectorDataT.select.where(_.vector_id eqs vecId).future()
    val resultRows = Await.result(future, 10 seconds).all()
    if (resultRows.size() > 1) {
      throw new Exception(s"Expected only one vector per vector id : $vecId")
    }
    if (resultRows.size() == 0) {
      throw new Exception(s"Expected a vector for vector id $vecId")
    }
    doubleVectorDataT.fromRow(resultRows.get(0)).vector_data.toVector
  }

  def putVector[Double](vecId : String, vec : Vector[Double]) = {
    val ins = doubleVectorDataT.insert().value(_.vector_id, vecId).value(_.vector_data, vec.toList).future().onComplete {
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

