package main.scala.core
import scala.collection.breakOut


trait Data {
  val data : Any // Todo make a general type?
}

class ScalarData[T](val data : T) extends Data
object DoubleScalarData{
  def apply(v : Double) = new ScalarData[Double](v)
}

class VectorData[T](val data : Vector[T]) extends Data
object DoubleVectorData {
  def apply(v : Vector[Double]) = new VectorData[Double](v)
}

class Matrix2DData[T](val data : Vector[Vector[T]]) extends Data
object DoubleMatrix2DData {
  def apply(v : Vector[Vector[Double]]) = new Matrix2DData[Double](v)
}

class TimeSeriesData[T](val data : Vector[T]) extends Data

class SingleChannelTimeSeriesData[T](val data : Vector[T], val times : Vector[Timestamp], val channel : TimeSeriesChannelId) extends Data {
}
object DoubleSingleChannelTimeSeriesData {
  def apply(data : Vector[Double], times : Vector[Timestamp], channel : TimeSeriesChannelId) =
    new SingleChannelTimeSeriesData[Double](data, times, channel)
}

class MultiChannelTimeSeriesData[T](
  val data : Vector[Vector[T]],
  val times : Vector[Timestamp],
  val channels : Vector[TimeSeriesChannelId]) extends Data{

  val channelToIndex  : Map[TimeSeriesChannelId, Int] = channels.zip(Range(0,channels.length))(breakOut)
  def getSingleChannel(channel : TimeSeriesChannelId) : SingleChannelTimeSeriesData[T] = {
    val ind : Int = channelToIndex.get(channel) match {
      case Some(ind) => ind
      case None => throw new Exception("Channel doesn't exist")
    }
    new SingleChannelTimeSeriesData[T](data(ind), times, channel)
  }
}
object DoubleMultiChannelTimeSeriesData {
  def apply(data : Vector[Vector[Double]], times : Vector[Timestamp], channels : Vector[TimeSeriesChannelId]) =
    new MultiChannelTimeSeriesData[Double](data, times, channels)
}
