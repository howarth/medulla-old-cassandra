package main.scala.sandbox
import main.scala.store._
import main.scala.core._
import scala.util.Random
import java.net.InetAddress
import java.nio.file.{Path,Paths,Files}

class SimpleCassandraStoreTest(hosts : Seq[String], keySpace : String){
  val rand = new Random(42)
  val cassStore = new CassandraStore(hosts, keySpace)
  val testScalars : IndexedSeq[Double] = (-10 to 20).map(_.toDouble).map(_ + .05)
  for(tc <- testScalars){
    cassStore.putScalar(ScalarId(tc.toString), DoubleScalarData(tc))
  }
  for(tc <- testScalars){
    assert(tc == cassStore.getScalar(ScalarId(tc.toString)).data)
  }

  val testVectors : Vector[Vector[Double]] = Vector.fill(100)(Vector.fill(rand.nextInt(100))(rand.nextDouble))
  for ((tv,i) <- testVectors.zipWithIndex) {
    cassStore.putVector(VectorId(i.toString), DoubleVectorData(tv))
  }
  for ((tv,i) <- testVectors.zipWithIndex) {
    assert(tv == cassStore.getVector(VectorId(i.toString)).data)
  }

  val testMatrices : Vector[Vector[Vector[Double]]] = Vector.fill(10)(
    Vector.fill(100)(
      Vector.fill(100)(rand.nextDouble)
    )
  )
  for ((tm, i) <- testMatrices.zipWithIndex){
    cassStore.putMatrix2D(Matrix2DId(i.toString), DoubleMatrix2DData(tm))
  }
  for ((tm, i) <- testMatrices.zipWithIndex){
    assert(tm == cassStore.getMatrix2D(Matrix2DId(i.toString)).data)
  }

  for ((tv,i) <- testVectors.zipWithIndex) {
    cassStore.putSingleChannelTimeSeries(SingleChannelTimeSeriesId(i.toString),
      DoubleSingleChannelTimeSeriesData(tv, (0 to tv.length).map(t => Timestamp(t.toString)).toVector,
        TimeSeriesChannelId(i.toString)))
  }
  for ((tv,i) <- testVectors.zipWithIndex) {
    val scts : SingleChannelTimeSeriesData[Double] = cassStore.getSingleChannelTimeSeries(SingleChannelTimeSeriesId(i.toString))
    assert(scts.channel.id == i.toString)
    assert(scts.times == (0 to tv.length).map(t => Timestamp(t.toString)).toVector)
    assert(scts.data == tv)
  }

  for ((tm,i) <- testMatrices.zipWithIndex) {
    cassStore.putMultiChannelTimeSeries(MultiChannelTimeSeriesId(i.toString),
      DoubleMultiChannelTimeSeriesData(tm, (0 to tm(0).length).map(t => Timestamp(t.toString)).toVector,
        TimeSeriesChannelId(i.toString)))
  }
  for ((tm,i) <- testMatrices.zipWithIndex) {
    val mcts : MultiChannelTimeSeriesData[Double] = cassStore.getMultiChannelTimeSeries(MultiChannelTimeSeriesId(i.toString))
    assert(mcts.channel.id == i.toString)
    assert(mcts.times == (0 to tm(0).length).map(t => Timestamp(t.toString)).toVector)
    assert(mcts.data == tm)
  }
}


/*

object HostnameDependentContext {
  val hostname = InetAddress.getLocalHost.getHostName
  val dataBasePathString = hostname match {
    case "eggss-MacBook-Pro.local" | "eggs.pc.cs.cmu.edu" => "/Users/dhowarth/work/db/data"
    case "srvtch-linux" => "/home/dhowarth/data/meg"
    case _ => throw new IllegalArgumentException(s"Unknown hostname $hostname")
  }
  val metadataBasePathString = hostname match {
    case "eggss-MacBook-Pro.local"  | "eggs.pc.cs.cmu.edu" => "/Users/dhowarth/work/labmetadata"
    case "srvtch-linux" => "/home/dhowarth/labmetadata"
    case _ => throw new IllegalArgumentException(s"Unknown hostname $hostname")
  }
}

object CheckAllRawData {
  val dataBasePathString = HostnameDependentContext.dataBasePathString
  val metadataBasePathString = HostnameDependentContext.metadataBasePathString
  val locator = new AlphaFIFFSDataLocator(dataBasePathString)
  val dataContext = new FIFDataContext(locator)
  val procSlug = new ProcessSlugId("raw")
  val metaContext = new AlphaFileSystemESBMetadataContext(metadataBasePathString)
  val allRecordings = metaContext.getAllRecordings()
  val allFilenames = allRecordings.map(locator.getRecordingLocation(_, procSlug))
  val doExist = allRecordings.filter(dataContext.recordingExists(_, procSlug))
  val dontExist = allRecordings.filter(!dataContext.recordingExists(_, procSlug))
  val filenames = dontExist.map(locator.getRecordingLocation(_, procSlug))
}

class PrepareWriteChannelsToMetadata() {
  val fiffLocator = new AlphaFIFFSDataLocator(HostnameDependentContext.dataBasePathString)
  val binLocator = new AlphaBinDataLocator(HostnameDependentContext.dataBasePathString)
  val channelId = new RecordingChannelId("CHANNEL-NAME")
  def writeFIFFToBinVector(outputPathString : String, recordingIds : Vector[RecordingId], processSlug: ProcessSlugId): Unit = {
    val fiffPaths : Vector[String] = recordingIds.map(fiffLocator.getRecordingLocation(_, processSlug).toString)
    val binTemplates : Vector[String] = recordingIds.map(binLocator.getRecordingChannelLocation(_,processSlug,channelId).toString)
    val outputStrings : Vector[String]  = fiffPaths.zip(binTemplates).map(fb => Array(fb._1, fb._2).mkString(" "))
    Files.write(Paths.get(outputPathString), outputStrings.mkString("\n").getBytes())
  }
}

class CheckTestDataBin {
  val fiffLocator = new AlphaFIFFSDataLocator(HostnameDependentContext.dataBasePathString)
  val binLocator = new AlphaBinDataLocator(HostnameDependentContext.dataBasePathString)
  val bdc = new BinDataContext(binLocator)
  val procSlug = new ProcessSlugId("raw")
  val recId = new RecordingId("test_A_01")
  val chans = bdc.getChannels(recId, procSlug)
  val times = bdc.getTimes(recId, procSlug)
  val startTime : Timestamp = bdc.getStartTime(recId, procSlug)
  val endTime : Timestamp = bdc.getEndTime(recId, procSlug)
  val chan = chans(0)
  val channelData = bdc.getChannelData(recId, procSlug,chan)

  assert(startTime == Timestamp("0"))
  assert(endTime == Timestamp("424.999"))
  assert(times == Vector.tabulate(425000)(n => Timestamp((n/1000.0).toString)))
}

class CheckTestCass {
  val rdc = new CassandraRecordingDataContext(Seq("localhost"), "test")
  val procSlug = new ProcessSlugId("raw")
  val recId = new RecordingId("test_A_01")
  val startTime : Timestamp = Timestamp("0")
  val endTime : Timestamp = Timestamp("424.999")
}

class CopyTestDataBinToCass(hosts : Seq[String], keySpace : String){
  val binLocator = new AlphaBinDataLocator(HostnameDependentContext.dataBasePathString)
  val bdc = new BinDataContext(binLocator)
  val procSlug = new ProcessSlugId("raw")
  val recId = new RecordingId("test_A_01")
  val chans = bdc.getChannels(recId, procSlug).slice(0,4)
  val times = bdc.getTimes(recId, procSlug)

  val rdc = new CassandraRecordingDataContext(Seq("localhost"), "test")


  /*
  for (c <- chans){
    println(c)
    val channelData = bdc.getChannelData(recId, procSlug, c)
    rdc.putChannelData(recId, procSlug, channelData)
  }
  rdc.db.putChannels(recId.getId, procSlug.getId, chans.map(_.getId).toList)
  rdc.db.putTimes(recId.getId(),times)
*/

}

*/

//424.999