package de.hpi.tfm.data.wikipedia.infobox

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.data.socrata.json.custom_serializer.{LocalDateKeySerializer, LocalDateSerializer}
import de.hpi.tfm.data.socrata.metadata.Provenance
import de.hpi.tfm.data.socrata.{JsonReadable, JsonWritable}
import de.hpi.tfm.data.wikipedia.infobox.InfoboxRevision.rename
import org.json4s.{DefaultFormats, FieldSerializer}
import org.json4s.FieldSerializer.{renameFrom, renameTo}
import org.json4s.ext.EnumNameSerializer

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

case class InfoboxRevision(revisionId:BigInt,
                           template:Option[String]=None,
                           pageTitle:String,
                           changes:IndexedSeq[InfoboxChange],
                           attributes:Option[Map[String,String]],
                           validFrom:String,
                           position:Option[Int]=None,
                           pageID:BigInt,
                           revisionType:Option[String],
                           user:Option[User]=None,
                           key:String,
                           validTo:Option[String]=None
) extends JsonWritable[InfoboxRevision] with StrictLogging{

  def checkIntegrity() ={
    changes.forall(c => c.currentValue.isEmpty || c.previousValue.isEmpty || c.currentValue.get!=c.previousValue.get)
    if(revisionType.isDefined){
      assert(revisionType.get=="DELETE" ^ attributes.isDefined) //^ = xor
      if(revisionType.get=="DELETE")
        assert(changes.forall(_.currentValue.isEmpty))
      if(revisionType.get=="CREATE")
        assert(changes.forall(_.previousValue.isEmpty))
    }
  }

  //May 15, 2012 11:16:19 PM
  //'2014-08-16T13:39:23Z'
  def stringToDate(str:String):LocalDateTime = {
    LocalDateTime.parse(str,InfoboxRevision.formatter)
  }


  override implicit def formats = InfoboxRevision.formats

  def validFromAsDate  = stringToDate(validFrom)
}

object InfoboxRevision extends JsonReadable[InfoboxRevision] with StrictLogging {

  val formatter: DateTimeFormatter = DateTimeFormatter.ISO_INSTANT//.ofPattern("MMM d[d], yyyy h[h]:mm:ss a")

  val rename = FieldSerializer[InfoboxRevision](renameTo("type", "revisionType"),renameFrom("type", "revisionType"))

  override implicit def formats = (DefaultFormats.preservingEmptyValues
    + LocalDateSerializer
    + LocalDateKeySerializer
    + rename)

  def toPaddedInfoboxHistory(objects: collection.Seq[InfoboxRevision]) = {
    val byKey = objects.groupBy(_.key)
    val todo = byKey.size
    logger.debug(s"Found $todo infobox lineages to create")
    var done = 0
    byKey.map(k => {
      val res = InfoboxRevisionHistory(k._1,k._2).toPaddedInfoboxHistory
      done +=1
      if(done%100==0) {
        logger.debug(s"Done with $done")
      }
      res
    })
  }

}
