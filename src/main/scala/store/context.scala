package main.scala.store
import main.scala.core._

class AlphaContext(
  val dataRegistry : DataRegistry,
  val dataStore : DataStore with
    ScalarRWDataStore[Double] with
    VectorRWDataStore[Double] with
    Matrix2DRWDataStore[Double] with
    SingleChannelTimeSeriesRWDataStore[Double] with
    MultiChannelTimeSeriesRWDataStore[Double]
  ) extends DataContext
{

  def put(id : DataId, data : Data) : Unit = {
    dataRegistry.registerData(id, data.metadata)
    (id, data) match {
      case (scalarId: ScalarId, scalarData: DoubleScalarData) => dataStore.putScalar(scalarId, scalarData)
      case (vectorId: VectorId, vectorData: DoubleVectorData) => dataStore.putVector(vectorId, vectorData)
      case (matrixId: Matrix2DId, matrixData: DoubleMatrix2DData) => dataStore.putMatrix2D(matrixId, matrixData)
      case (tsId: SingleChannelTimeSeriesId,
            timeSeriesData: DoubleSingleChannelTimeSeriesData) =>
        dataStore.putSingleChannelTimeSeries(tsId, timeSeriesData)
      case (tsId: MultiChannelTimeSeriesId,
            timeSeriesData: DoubleMultiChannelTimeSeriesData) =>
        dataStore.putMultiChannelTimeSeries(tsId, timeSeriesData)
      case _ => throw new Exception("Unknown DataId type and DataType")
    }
  }

  def get(id : DataId): Data ={
    if(!dataRegistry.dataIsRegistered(id)){
      throw new Exception(s"Data does not exist for DataId $id")
    }
    id match {
      case (scalarId: ScalarId) => dataStore.getScalar(scalarId)
      case (vectorId: VectorId) => dataStore.getVector(vectorId)
      case (matrixId: Matrix2DId) => dataStore.getMatrix2D(matrixId)
      case (tsId: SingleChannelTimeSeriesId) => dataStore.getSingleChannelTimeSeries(tsId)
      case (tsId: MultiChannelTimeSeriesId) =>
        dataStore.getMultiChannelTimeSeries(tsId)
      case _ => throw new Exception("Unknown DataId")
    }

  }

  def delete(id : DataId) : Unit = {
    dataRegistry.deleteData(id)
    id match {
      case (scalarId: ScalarId) => dataStore.deleteScalar(scalarId)
      case (vectorId: VectorId) => dataStore.deleteVector(vectorId)
      case (matrixId: Matrix2DId) => dataStore.deleteMatrix2D(matrixId)
      case (tsId: SingleChannelTimeSeriesId) => dataStore.deleteSingleChannelTimeSeries(tsId)
      case (tsId: MultiChannelTimeSeriesId) =>
        dataStore.deleteMultiChannelTimeSeries(tsId)
      case _ => throw new Exception("Unknown DataId type")
    }
  }
}

