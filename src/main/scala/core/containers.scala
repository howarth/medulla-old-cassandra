package main.scala.core
import scala.collection.breakOut

class SingleChannelTimeseries(val data : Vector[Double], val time : Vector[Timestamp], val channel : RecordingChannelId) {
}

class MultiChannelTimeseries(val data : Vector[Vector[Double]], val time : Vector[Timestamp], val channels : Vector[RecordingChannelId]) {
  val channelToIndex  : Map[RecordingChannelId, Int] = channels.zip(Range(0,channels.length))(breakOut)
  def getSingleChannel(channel : RecordingChannelId) : SingleChannelTimeseries = {
    val ind : Int = channelToIndex.get(channel) match {
      case Some(ind) => ind
      case None => throw new Exception("Channel doesn't exist")
    }
    new SingleChannelTimeseries(data(ind), time, channel)
  }
}
