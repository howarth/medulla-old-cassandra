import org.scalatest._
import main.scala.core._


class AlphaFSMetadataContextSpec extends FlatSpec with Matchers{

  val alphaParser = AlphaRecordingIdParser
  val labmetadataPath = "/Users/dhowarth/work/labmetadata/"
  val context = new AlphaFileSystemESBMetadataContext(labmetadataPath)
  val testingExperiment = new ExperimentId("testing")

  "the list of experiments" should "contain 'testing'" in {
    val experiments = context.getExperiments()
    println(experiments)
    (experiments.contains(testingExperiment)) should be (true)
  }

  "Subject A from Experiment testing" should " have data 01, 02, 03, 04" in {
    val subject = new SubjectId("A")
    val recordings : Vector[RecordingId] = context.getDataRecordings(testingExperiment, subject)
    def bid2rid(b : BlockId) : RecordingId = alphaParser.combine(testingExperiment)(subject)(b)
    val trueRecordings = Vector("01","02","03","04").map(s => bid2rid(new BlockId(s)))
    trueRecordings.equals(recordings) should be (true)
  }

  "Emptyroom recorings for testing" should "be correct..." in {
    val subject = new SubjectId("B")
    val b2r : ( String => RecordingId) =
      (s :String) => alphaParser.combine(testingExperiment)(subject)(new BlockId(s))
    val rid2erid = context.getEmptyRoomRecordingsMap(testingExperiment, subject)

    val erA = b2r("EmptyRoomA")
    val erB = b2r("EmptyRoomB")
    rid2erid.equals(
      Map(b2r("01") -> erA, b2r("02") -> erA,
        b2r("03") -> erB, b2r("04") -> erB)
    )
    1 should be (1)
  }
}
