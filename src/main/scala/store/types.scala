package main.scala.store


import main.scala.core._
trait DataStore
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
}
trait Matrix2DRWDataStore[T] extends Matrix2DWritableDataStore[T] with Matrix2DReadableDataStore[T]

/*
 TimeSeries
 */
trait TimeSeriesReadableDataStore[T] extends ReadableDataStore {
  def getTimeSeries(id : TimeSeriesId) : TimeSeriesData[T]
}
trait TimeSeriesWritableDataStore[T] extends WritableDataStore {
  def putTimeSeries(id : TimeSeriesId, data : TimeSeriesData[T]) : Unit
}
trait TimeSeriesRWDataStore[T] extends TimeSeriesReadableDataStore[T] with TimeSeriesWritableDataStore[T]

trait SingleChannelTimeSeriesReadableDataStore[T] extends WritableDataStore {
  def getSingleChannelTimeSeries(id : SingleChannelTimeSeriesId): SingleChannelTimeSeriesData[T]
}
trait SingleChannelTimeSeriesWritableDataStore[T] extends {
  def putSingleChannelTimeSeries(id : SingleChannelTimeSeriesId, data : SingleChannelTimeSeriesData[T]) : Unit
}
trait SingleChannelTimeSeriesRWDataStore[T] extends
  SingleChannelTimeSeriesReadableDataStore[T] with
  SingleChannelTimeSeriesWritableDataStore[T]

trait MultiChannelTimeSeriesReadableDataStore[T] extends ReadableDataStore {
  def getMultiChannelTimeSeriesStore(id: MultiChannelTimeSeriesId): MultiChannelTimeSeriesData[T]
}
trait MultiChannelTimeSeriesWritableDataStore[T] extends WritableDataStore {
  def putMultiChannelTimeSeriesStore(id: MultiChannelTimeSeriesId, data : MultiChannelTimeSeriesData[T] ): Unit
}
trait MultiChannelTimeSeriesRWDataStore[T] extends
  MultiChannelTimeSeriesReadableDataStore[T] with
  MultiChannelTimeSeriesWritableDataStore[T]
