package main.scala.core


trait Data {
  def getData() : Data
}

trait Vector[T] extends Data{
  def getData() : Vector[T]
}

trait Matrix2D[T] extends Data{
  def getData() : Vector[Vector[T]]
}

trait Timeseries[T] extends Data{
  def getData() : SingleChannelTimeseries
}