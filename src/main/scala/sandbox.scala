package main.scala.sandbox
import main.scala.core._
import java.net.InetAddress


object HostnameDependentContext {
  val hostname = InetAddress.getLocalHost.getHostName
  val dataBasePathString = hostname match {
    case "eggss-MacBook-Pro.local" => "/Users/dhowarth/work/db/data"
    case _ => throw new IllegalArgumentException("Unknown hostname")
  }
  val metadataBasePathString = hostname match {
    case "eggss-MacBook-Pro.local" => "/Users/dhowarth/work/labmetadata"
    case _ => throw new IllegalArgumentException("Unknown hostname")
  }
}

object CheckAllRawData {
  val dataBasePathString = HostnameDependentContext.dataBasePathString
  val metadataBasePathString = HostnameDependentContext.metadataBasePathString
  val locator = new AlphaFSDataLocator(dataBasePathString)
  val dataContext = new FSDataContext(locator)
  val procSlug = new ProcessSlugIdentifier("raw")
  val metaContext = new AlphaFSMetadataContext(metadataBasePathString)
  val allRecordings = metaContext.getAllRecordings()
  val allFilenames = allRecordings.map(locator.getRecordingLocation(_, procSlug))
  val doExist = allRecordings.filter(dataContext.recordingExists(_, procSlug))
  val dontExist = allRecordings.filter(!dataContext.recordingExists(_, procSlug))
  val filenames = dontExist.map(locator.getRecordingLocation(_, procSlug))
}

object WriteStimuliChannelsToMetadata {
  def writeListForMNEInput
}
