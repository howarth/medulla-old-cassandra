package main.scala.sandbox
import main.scala.core._

object CheckAllRawDataOnCortex {
  val dataBasePathString = "/share/volume0/newmeg"
  val metadataBasePathString = "/home/dhowarth/labmetadata/"
  val locator = new AlphaFSDataLocator(dataBasePathString)
  val dataContext = new FSDataContext(locator)
  val procSlug = new ProcessSlugIdentifier("raw")
  val metaContext = new AlphaFSMetadataContext(metadataBasePathString)
  val allRecordings = metaContext.getAllRecordings()
  val dontExist = allRecordings.filter(!dataContext.recordingExists(_, procSlug))
}
