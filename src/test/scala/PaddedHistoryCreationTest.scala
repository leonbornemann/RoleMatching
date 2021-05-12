

object PaddedHistoryCreationTest extends App {
  //revisionId:BigInt,
  //                           template:Option[String]=None,
  //                           pageTitle:String,
  //                           changes:IndexedSeq[InfoboxChange],
  //                           attributes:Option[Map[String,String]],
  //                           validFrom:String,
  //                           position:Option[Int]=None,
  //                           pageID:BigInt,
  //                           revisionType:Option[String],
  //                           user:Option[User]=None,
  //                           key:String,
  //                           validTo:Option[String]=None
  //)
  var curRevisionID = 0
  val title = "Title-Test"
  //TODO: figure out a good way to test the extraction
  val hist1 = "__A_B_C_D___E"
  val hist2 = "C_A_B_C_D___E"

  def nextRevisionID() = {
    curRevisionID +=1
    curRevisionID
  }

  //val revision1 = InfoboxRevision(nextRevisionID(),)

}
