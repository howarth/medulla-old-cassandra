package main.scala.store
import main.scala.core._
import com.outworkers.phantom.dsl._
import scala.util.{Failure, Success}
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

class CassandraStore(hosts : Seq[String], keySpaceString : String) extends
  ScalarRWDataStore[Double] with
  VectorRWDataStore[Double] with
  Matrix2DRWDataStore[Double] with
  SingleChannelTimeSeriesRWDataStore[Double] with
  MultiChannelTimeSeriesRWDataStore[Double] {

  val connector : KeySpaceDef = ContactPoint.local.noHeartbeat().keySpace(keySpaceString)
  val db = new CassPhantomDatabase(connector)

  override def putScalar(id: ScalarId, data: ScalarData[Double]) = db.putDoubleScalar(id.id, data.data)
  override def getScalar(id: ScalarId) : ScalarData[Double] = DoubleScalarData(db.getDoubleScalar(id.id))

  override def putVector(id: VectorId, data: VectorData[Double]) = db.putDoubleVector(id.id, data.data)
  override def getVector(id: VectorId) = DoubleVectorData(db.getDoubleVector(id.id))

  override def getMatrix2D(id: Matrix2DId) = DoubleMatrix2DData(db.getDoubleMatrix2D(id.id))
  override def putMatrix2D(id: Matrix2DId, data: Matrix2DData[Double]) = db.putDoubleMatrix2D(id.id, data.data)

  override def getSingleChannelTimeSeries(id: SingleChannelTimeSeriesId) : SingleChannelTimeSeriesData[Double] = {
    val chan = db.getChannels(id.id)
    if(chan.length != 1) throw new Error
    val bounds = db.getTimeSeriesTimeBounds(id.id)
    val times : Vector[Timestamp] = db.getTimes(id.id).map(Timestamp(_))
    val data : Vector[Double] = db.getChannelData(id.id, chan(0), bounds._1, bounds._2)
    DoubleSingleChannelTimeSeriesData(data, times, TimeSeriesChannelId(chan(0)))
  }

  override def putSingleChannelTimeSeries(id: SingleChannelTimeSeriesId,
    data : SingleChannelTimeSeriesData[Double]) = {
    // TODO transaction or try
    db.putTimeBounds(id.id, data.times.head.underlyingBD, data.times.last.underlyingBD)
    db.putTimes(id.id, data.times.map(_.underlyingBD))
    db.putChannels(id.id, List(data.channel.id))
    db.putChannelData(id.id, data.channel.id, data.times.map(_.underlyingBD), data.data)
  }

  def dataExists(id: main.scala.core.DataId): Unit = throw new NotImplementedError
  def getChannels(id: main.scala.core.MultiChannelTimeSeriesId): Vector[main.scala.core.TimeSeriesChannelId] = throw new NotImplementedError
  def getChannel(id: main.scala.core.SingleChannelTimeSeriesId): main.scala.core.TimeSeriesChannelId = throw new NotImplementedError
  def getFirstTimestamp(id: main.scala.core.TimeSeriesId): main.scala.core.Timestamp =  throw new NotImplementedError
  def getLastTimestamp(id: main.scala.core.TimeSeriesId): main.scala.core.Timestamp =  throw new NotImplementedError
  def getTimes(id: main.scala.core.TimeSeriesId): Vector[main.scala.core.Timestamp] =  throw new NotImplementedError

  override def getMultiChannelTimeSeries(id: MultiChannelTimeSeriesId) = {

  }
  override def putMultiChannelTimeSeries(id: MultiChannelTimeSeriesId, data: MultiChannelTimeSeriesData[Double]) = throw new NotImplementedError
}

/*
create table if not exists double_scalar_data(
  id text,
  data double,
  PRIMARY KEY(id),
)
*/
case class DoubleScalarRecord(id: String, data : Double)
class DoubleScalarTable extends CassandraTable[DoubleScalarTable, DoubleScalarRecord]{
  override val tableName = "double_scalar_data"
  object id extends StringColumn(this) with PartitionKey
  object data extends DoubleColumn(this)
}

/*
create table if not exists double_vector_data(
  id text,
  data frozen<list<double>>,
  PRIMARY KEY(id),
);
*/
case class DoubleVectorRecord(id: String, data : List[Double])
class DoubleVectorTable extends CassandraTable[DoubleVectorTable, DoubleVectorRecord]{
  override val tableName = "double_vector_data"
  object id extends StringColumn(this) with PartitionKey
  object data extends ListColumn[Double](this)
}

/*
create table if not exists double_matrix2D_data(
  id text,
  data frozen<list<double>>,
  PRIMARY KEY(id),
);
*/
case class DoubleMatrix2DDataRow(id : String, data : List[Double])
class DoubleMatrix2DTable extends CassandraTable[DoubleMatrix2DTable, DoubleMatrix2DDataRow]{
  override val tableName = "double_matrix2D_data"
  object id extends StringColumn(this) with PartitionKey
  object data extends ListColumn[Double](this)
}

/*
create table if not exists double_timeseries_data(
  id text,
  timestamp decimal,
  value double,
  PRIMARY KEY(id, timestamp),
) WITH CLUSTERING ORDER BY (timestamp asc);
*/


/*
create table if not exists double_channeled_timeseries_data(
  id text,
  channel_name text,
  timestamp decimal,
  value double,
  PRIMARY KEY ((id,channel_name), timestamp),
 ) WITH CLUSTERING ORDER BY (timestamp asc);
*/
case class DoubleChanneledTimeSeriesDataRecord(
                                       id : String,
                                       timestamp : BigDecimal,
                                       channel_name : String,
                                       value: Double)
class DoubleChanneledTimeSeriesDataTable extends
  CassandraTable[DoubleChanneledTimeSeriesDataTable, DoubleChanneledTimeSeriesDataRecord] {
  override val tableName = "double_channeled_timeseries_data"
  object id extends StringColumn(this) with PartitionKey
  object timestamp extends BigDecimalColumn(this) with PrimaryKey with ClusteringOrder with Ascending
  object channel_name extends StringColumn(this) with PrimaryKey with ClusteringOrder with Ascending
  object value extends DoubleColumn(this)
}

/*
create table if not exists timeseries_bounds(
  id text,
  start_time decimal,
  end_time decimal,
  PRIMARY KEY(id));
*/
case class TimeSeriesTimeBoundsRecord(id : String, start_time : BigDecimal, end_time : BigDecimal)
class TimeSeriesTimeBoundsTable extends CassandraTable[TimeSeriesTimeBoundsTable, TimeSeriesTimeBoundsRecord]{
  override val tableName = "timeseries_bounds"
  object id extends StringColumn(this) with PartitionKey
  object start_time extends BigDecimalColumn(this)
  object end_time extends BigDecimalColumn(this)
}

/*
create table if not exists timeseries_ids(
  id text,
  PRIMARY KEY(id)
);
*/
case class TimeSeriesIdRecord(id : String)
class TimeSeriesIdTable extends CassandraTable[TimeSeriesIdTable, TimeSeriesIdTable] {
  override val tableName = "timeseries_ids"
  object id extends StringColumn(this) with PartitionKey
}

/*
create table if not exists timeseries_timestamps(
  id text,
  times frozen<list<decimal>>,
  PRIMARY KEY(id)
);
*/
case class TimeSeriesTimestampsRecord(id : String, times : List[BigDecimal])
class TimeSeriesTimestampsTable extends CassandraTable[TimeSeriesTimestampsTable, TimeSeriesTimestampsRecord] {
  override val tableName = "timeseries_timestamps"
  object id extends StringColumn(this) with PartitionKey
  object times extends ListColumn[BigDecimal](this)
}

/*
create table if not exists timeseries_channels(
  id text,
  channel_names frozen<list<text>>,
  PRIMARY KEY(id)
);
 */
case class TimeSeriesChannelsRecord(id  : String, channel_names: List[String])
class TimeSeriesChannelsTable extends CassandraTable[TimeSeriesChannelsTable, TimeSeriesChannelsRecord] {
  override val tableName = "timeseries_channels"
  object id extends StringColumn(this) with PartitionKey
  object channel_names extends ListColumn[String](this)
}

class CassPhantomDatabase(override val connector : KeySpaceDef) extends Database[CassPhantomDatabase](connector) {
  object timeBoundsT extends TimeSeriesTimeBoundsTable with connector.Connector
  object timestampsT extends TimeSeriesTimestampsTable with connector.Connector
  object channelsT extends TimeSeriesChannelsTable with connector.Connector
  object doubleScalarDataT extends DoubleScalarTable with connector.Connector
  object doubleChanneledTimeSeriesDataT extends DoubleChanneledTimeSeriesDataTable with connector.Connector
  object doubleVectorDataT extends DoubleVectorTable with connector.Connector
  object doubleMatrix2DDataT extends DoubleMatrix2DTable with connector.Connector

  def getTimeSeriesTimeBounds(id : String) : Tuple2[BigDecimal, BigDecimal] = {
    val future = timeBoundsT.select.where(_.id eqs id).future()
    val resultRows  = Await.result(future, 10 seconds).all()
    if (resultRows.size() > 1) {
      throw new Exception("Expected one time bound for recording id")
    }
    if (resultRows.size() == 0) {
      throw new Exception("Expected one time bound for recording id, not found")
    }
    val tb = timeBoundsT.fromRow(resultRows.get(0))
    Tuple2(tb.start_time, tb.end_time)
  }

  def putTimes(id : String, times : Vector[BigDecimal]) : Unit =  {
    timestampsT.insert().value(_.id, id).value(_.times, times.toList)
      .future().onComplete {
      case Success(e) => ()
      case Failure(e) => throw e
    }
  }

  def getTimes(id : String) : Vector[BigDecimal] = {
    val resultRows = Await.result(timestampsT.select.where(_.id eqs id).future(), 10 seconds).all()
    if (resultRows.size() > 1) {
      throw new Exception("Expected one time list for recording id")
    }
    if (resultRows.size() == 0) {
      throw new Exception("Expected one time list for recording id, not found")
    }
    timestampsT.fromRow(resultRows.get(0)).times.toVector
  }

  def putTimeBounds(id : String,
                    startTime : BigDecimal, endTime : BigDecimal): Unit = {
    timeBoundsT.insert
      .value(_.id, id)
      .value(_.start_time, startTime)
      .value(_.end_time, endTime).future().onComplete {
      case Success(e) => ()
      case Failure(e) => throw e
    }
  }

  def putChannels(id : String, channels : List[String]) : Unit = {
    channelsT.insert.value(_.id, id)
      .value(_.channel_names, channels).future().onComplete {
      case Success(e) => ()
      case Failure(e) => throw e
    }
  }

  def getChannels(id : String) : List[String] ={
    val future = channelsT.select.where(_.id eqs id).future()
    val resultRows  = Await.result(future, 10 seconds).all()
    if (resultRows.size() > 1) {
      throw new Exception("Expected one channels for recording id")
    }
    if (resultRows.size() == 0) {
      throw new Exception("Expected one channels for recording id, not found")
    }
    channelsT.fromRow(resultRows.get(0)).channel_names

  }

  def putChannelData(id : String, channel : String, times : Vector[BigDecimal], data: Vector[Double]) = {
    val ins = doubleChanneledTimeSeriesDataT.insert().value(_.id, id).value(_.channel_name, channel)
    val futures : Vector[Future[ResultSet]] = data.zip(times).map( z => z match {case (d : Double,t : BigDecimal) =>
      ins.value(_.timestamp, t).value(_.value, d).future()
    })
  }

  def getChannelData(id : String, channel : String, startTime : BigDecimal, endTime : BigDecimal) : Vector[Double] = {
    val future = doubleChanneledTimeSeriesDataT.select
      .where(_.id eqs id)
      .and(_.channel_name eqs channel).and(_.timestamp gte startTime).and(_.timestamp lte endTime).orderBy(t => t.timestamp asc).future()
    val resultRows = Await.result(future, 10 seconds).all()
    Vector.tabulate[Double](resultRows.size()){ri => doubleChanneledTimeSeriesDataT.fromRow(resultRows.get(ri)).value}
  }

  def getDoubleScalar(id : String) : Double = {
    val future = doubleScalarDataT.select.where(_.id eqs id).future()
    val resultRows = Await.result(future, 10 seconds).all()
    if (resultRows.size() > 1) {
      throw new Exception(s"Expected only one vector per vector id : $id")
    }
    if (resultRows.size() == 0) {
      throw new Exception(s"Expected a vector for vector id $id")
    }
    doubleScalarDataT.fromRow(resultRows.get(0)).data
  }

  def putDoubleScalar(id : String, scalar : Double) : Unit = {
    val ins = doubleScalarDataT.insert().value(_.id, id).value(_.data, scalar).future().onComplete {
      case Success(e) => ()
      case Failure(e) => throw e
    }
  }

  def getDoubleVector(id: String) : Vector[Double]= {
    val future = doubleVectorDataT.select.where(_.id eqs id).future()
    val resultRows = Await.result(future, 10 seconds).all()
    if (resultRows.size() > 1) {
      throw new Exception(s"Expected only one vector per vector id : $id")
    }
    if (resultRows.size() == 0) {
      throw new Exception(s"Expected a vector for vector id $id")
    }
    val vec : List[Double] = doubleVectorDataT.fromRow(resultRows.get(0)).data
    vec.toVector
  }

  def putDoubleVector(id : String, vec : Vector[Double]) = {
    val ins = doubleVectorDataT.insert().value(_.id, id).value(_.data, vec.toList).future().onComplete {
      case Success(e) => ()
      case Failure(e) => throw e
    }
  }

  def getDoubleMatrix2D(id: String) : Vector[Vector[Double]]= {
    val future = doubleMatrix2DDataT.select.where(_.id eqs id).future()
    val resultRows = Await.result(future, 10 seconds).all()
    if (resultRows.size() > 1) {
      throw new Exception(s"Expected only one matrix per matrix id : $id")
    }
    if (resultRows.size() == 0) {
      throw new Exception(s"Expected a matrix for matrix id $id")
    }
    val data : Vector[Double] = doubleMatrix2DDataT.fromRow(resultRows.get(0)).data.toVector
    val firstDim : Int = data(0).toInt
    val secondDim : Int = data(1).toInt
    (0 to (firstDim-1)).map(i => data.slice(2+(i*secondDim), 2+(i*secondDim)+secondDim)).toVector
  }

  def putDoubleMatrix2D(id : String, matrix : Vector[Vector[Double]]) = {
    val firstDim = matrix.length
    val secondDim = matrix(0).length
    val dbData : List[Double] = List(firstDim.toDouble, secondDim.toDouble) ++ matrix.flatten.toList
    val ins = doubleMatrix2DDataT.insert()
      .value(_.id, id)
      .value(_.data, dbData).future().onComplete {
      case Success(e) => ()
      case Failure(e) => throw e
    }
  }

}

