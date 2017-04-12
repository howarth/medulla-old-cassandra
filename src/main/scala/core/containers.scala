package main.scala.core
import scala.collection.breakOut

trait CanBeVectorized[T] {
  def vecotrize() : VecotrData[T]
}

trait Data {
  val data : Any // Todo make a general type?
}

class VectorData[T](val data : Vector[T]) extends Data

class Matrix2DData[T](val data : Vector[Vector[T]]) extends Data

class TimeSeriesData[T](val data : Vector[T]) extends Data

class SingleChannelTimeSeriesData[T](val data : Vector[T], val time : Vector[T], val channel : TimeseriesChannelId) extends Data {
}

class MultiChannelTimeSeriesData[T](
  val data : Vector[Vector[T]],
  val time : Vector[T],
  val channels : Vector[TimeseriesChannelId]) extends Data{

  val channelToIndex  : Map[RecordingChannelId, Int] = channels.zip(Range(0,channels.length))(breakOut)
  def getSingleChannel(channel : RecordingChannelId) : SingleChannelTimeseries[T] = {
    val ind : Int = channelToIndex.get(channel) match {
      case Some(ind) => ind
      case None => throw new Exception("Channel doesn't exist")
    }
    new SingleChannelTimeseries[T](data(ind), time, channel)
  }
}
