package main.scala.store

trait Matrix2DStore {
}



trait Matrix2DReadableStore[T] extends Matrix2DStore{
  def getMatrix2D(id : Matrix2DId) : Matrix2D[T]
}

trait Matrix2DWritableStore extends Matrix2DStore{
  def putMatrix2D(id : Matrix2DId, data : Matrix2D)
}


