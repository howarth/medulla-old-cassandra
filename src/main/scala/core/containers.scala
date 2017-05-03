package main.scala.core
import scala.collection.breakOut


trait Data {
  val data : Any // Todo make a general type?
  val metadata : Metadata
}
trait Metadata {
}
trait HomogenousDataMetadata[T] extends Metadata {
  // Todo learn about types
  ///val dataType = T
}

abstract class ScalarData[T](val data : T) extends Data {
  lazy val metadata = DoubleScalarMetadata()
}

class DoubleScalarData(data : Double) extends ScalarData[Double](data)
object DoubleScalarData{
  def apply(v : Double) = new DoubleScalarData(v)
}

class ScalarMetadata[T] extends HomogenousDataMetadata[T]
class DoubleScalarMetadata extends ScalarMetadata[Double]
object DoubleScalarMetadata {
  def apply() = new DoubleScalarMetadata
}


abstract class VectorData[T](val data : Vector[T]) extends Data
class DoubleVectorData(data : Vector[Double]) extends VectorData[Double](data){
  val metadata = DoubleVectorMetadata(data.length)
}
object DoubleVectorData {
  def apply(v : Vector[Double]) = new DoubleVectorData(v)
}
class VectorMetadata[T](val length : Int) extends HomogenousDataMetadata[T]
class DoubleVectorMetadata(length : Int) extends VectorMetadata[Double](length)
object DoubleVectorMetadata {
  def apply(length : Int) = new DoubleVectorMetadata(length)
}

abstract class Matrix2DData[T](val data : Vector[Vector[T]]) extends Data {
}

class DoubleMatrix2DData(data : Vector[Vector[Double]])
  extends Matrix2DData[Double](data){

  val metadata = DoubleMatrix2DMetadata(Tuple2(data.length, data(0).length))
}
object DoubleMatrix2DData {
  def apply(v : Vector[Vector[Double]]) = new DoubleMatrix2DData(v)
}

class Matrix2DMetadata[T](val shape : Tuple2[Int,Int]) extends HomogenousDataMetadata[T]
class DoubleMatrix2DMetadata(shape : Tuple2[Int,Int]) extends Matrix2DMetadata[Double](shape)
object DoubleMatrix2DMetadata {
  def apply(shape : Tuple2[Int,Int]) = new DoubleMatrix2DMetadata(shape)
}


abstract class SingleChannelTimeSeriesData[T](
    val data : Vector[T],
    val times : Vector[Timestamp],
    val channel : TimeSeriesChannelId)
  extends Data {
}
class DoubleSingleChannelTimeSeriesData(
    data : Vector[Double],
    times : Vector[Timestamp],
    channel : TimeSeriesChannelId)
  extends SingleChannelTimeSeriesData[Double](data, times, channel){

  val metadata = DoubleSingleChannelTimeSeriesMetadata(times.length)
}
object DoubleSingleChannelTimeSeriesData {
  def apply(data : Vector[Double], times : Vector[Timestamp], channel : TimeSeriesChannelId) =
    new DoubleSingleChannelTimeSeriesData(data, times, channel)
}

class SingleChannelTimeSeriesMetadata[T](val length : Int) extends HomogenousDataMetadata[T]
class DoubleSingleChannelTimeSeriesMetadata(length : Int) extends SingleChannelTimeSeriesMetadata[Double](length)
object DoubleSingleChannelTimeSeriesMetadata {
  def apply(length : Int) = new DoubleSingleChannelTimeSeriesMetadata(length)
}

abstract class MultiChannelTimeSeriesData[T](
  val data : Vector[Vector[T]],
  val times : Vector[Timestamp],
  val channels : Vector[TimeSeriesChannelId]) extends Data{

  val channelToIndex  : Map[TimeSeriesChannelId, Int] = channels.zip(Range(0,channels.length))(breakOut)
}

class DoubleMultiChannelTimeSeriesData(data : Vector[Vector[Double]],
    times : Vector[Timestamp],
    channels : Vector[TimeSeriesChannelId])
  extends MultiChannelTimeSeriesData[Double](data, times, channels){

  val metadata = DoubleMultiChannelTimeSeriesMetadata(times.length, data.length)

  def getSingleChannel(channel : TimeSeriesChannelId) : DoubleSingleChannelTimeSeriesData = {
    val ind : Int = channelToIndex.get(channel) match {
      case Some(ind) => ind
      case None => throw new Exception("Channel doesn't exist")
    }
    new DoubleSingleChannelTimeSeriesData(data(ind), times, channel)
  }
}
object DoubleMultiChannelTimeSeriesData {
  def apply(data : Vector[Vector[Double]], times : Vector[Timestamp], channels : Vector[TimeSeriesChannelId]) =
    new DoubleMultiChannelTimeSeriesData(data, times, channels)
}
class MultiChannelTimeSeriesMetadata[T](val length : Int, val nChannels : Int) extends HomogenousDataMetadata[T]
class DoubleMultiChannelTimeSeriesMetadata(length : Int, nChannels : Int) extends MultiChannelTimeSeriesMetadata[Double](length, nChannels)
object DoubleMultiChannelTimeSeriesMetadata {
  def apply(length : Int, nChannels : Int) = new DoubleMultiChannelTimeSeriesMetadata(length, nChannels)
}
