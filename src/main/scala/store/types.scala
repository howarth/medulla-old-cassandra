package main.scala.store
import main.scala.core._

trait DataContext {
  val dataRegistry : DataRegistry
  val dataStore : DataStore
}

trait DataRegistry{
  def dataIsRegistered(id : DataId) : Boolean
  def registerData(id : DataId, structMeta : Metadata) : Unit
  def deleteData(id : DataId)
  def getMetadata(id : DataId) : Metadata
}

trait DataStore {
  //def dataExists(id : DataId) : Boolean
}
trait ReadableDataStore extends DataStore
trait WritableDataStore extends DataStore

/* thoughts as I go here
should an ID have the type in it?
is there a way to do something like
trait ReadableDataStore {
  get[Data][T]
}
and then implement
get[Matrix2D][Double]
and then not write as much?
*/

/*
  Scalars
 */
trait ScalarStore[T]
trait ScalarReadableDataStore[T] extends ScalarStore[T] with ReadableDataStore{
  def getScalar(id : ScalarId): ScalarData[T]
}
trait ScalarWritableDataStore[T] extends ScalarStore[T] with WritableDataStore{
  def putScalar(id : ScalarId, data : ScalarData[T]) : Unit
  def deleteScalar(id : ScalarId) : Unit
}
trait ScalarRWDataStore[T] extends ScalarReadableDataStore[T] with ScalarWritableDataStore[T]


/*
  Vectors
  */
trait VectorStore[T]
trait VectorReadableDataStore[T] extends VectorStore[T] with ReadableDataStore{
  def getVector(id : VectorId): VectorData[T]
}
trait VectorWritableDataStore[T] extends VectorStore[T] with WritableDataStore{
  def putVector(id : VectorId, data : VectorData[T]) : Unit
  def deleteVector(id : VectorId) : Unit
}
trait VectorRWDataStore[T] extends VectorReadableDataStore[T] with VectorWritableDataStore[T]

/*
  2D Matricies
 */
trait Matrix2DStore[T]
trait Matrix2DReadableDataStore[T] extends Matrix2DStore[T] with ReadableDataStore{
  def getMatrix2D(id : Matrix2DId) : Matrix2DData[T]
}
trait Matrix2DWritableDataStore[T] extends Matrix2DStore[T] with WritableDataStore{
  def putMatrix2D(id : Matrix2DId, data : Matrix2DData[T]) : Unit
  def deleteMatrix2D(id : Matrix2DId) : Unit
}
trait Matrix2DRWDataStore[T] extends Matrix2DWritableDataStore[T] with Matrix2DReadableDataStore[T]

/*
 TimeSeries
 */
trait TimeSeriesReadableDataStore[T] extends ReadableDataStore {
  def getFirstTimestamp(id : TimeSeriesId) : Timestamp
  def getLastTimestamp(id : TimeSeriesId) : Timestamp
  def getTimes(id : TimeSeriesId) : Vector[Timestamp]
}
trait TimeSeriesWritableDataStore[T] extends WritableDataStore {
}
trait TimeSeriesRWDataStore[T] extends TimeSeriesReadableDataStore[T] with TimeSeriesWritableDataStore[T]

trait SingleChannelTimeSeriesReadableDataStore[T] extends TimeSeriesReadableDataStore[T]{
  def getSingleChannelTimeSeries(id : SingleChannelTimeSeriesId): SingleChannelTimeSeriesData[T]
  def getChannel(id : SingleChannelTimeSeriesId) : TimeSeriesChannelId
}
trait SingleChannelTimeSeriesWritableDataStore[T] extends TimeSeriesWritableDataStore[T]{
  def putSingleChannelTimeSeries(id : SingleChannelTimeSeriesId, data : SingleChannelTimeSeriesData[T]) : Unit
  def deleteSingleChannelTimeSeries(id : SingleChannelTimeSeriesId) : Unit
}
trait SingleChannelTimeSeriesRWDataStore[T] extends
  SingleChannelTimeSeriesReadableDataStore[T] with
  SingleChannelTimeSeriesWritableDataStore[T]

trait MultiChannelTimeSeriesReadableDataStore[T] extends TimeSeriesReadableDataStore[T] {
  def getMultiChannelTimeSeries(id: MultiChannelTimeSeriesId): MultiChannelTimeSeriesData[T]
  def getChannels(id : MultiChannelTimeSeriesId): Vector[TimeSeriesChannelId]
}
trait MultiChannelTimeSeriesWritableDataStore[T] extends TimeSeriesWritableDataStore[T] {
  def putMultiChannelTimeSeries(id: MultiChannelTimeSeriesId, data : MultiChannelTimeSeriesData[T] ): Unit
  def deleteMultiChannelTimeSeries(id : MultiChannelTimeSeriesId) : Unit
}
trait MultiChannelTimeSeriesRWDataStore[T] extends
  MultiChannelTimeSeriesReadableDataStore[T] with
  MultiChannelTimeSeriesWritableDataStore[T]
