package de.hpi.wikipedia_data_preparation.original_infobox_data

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.cbrm.data.json_serialization.{JsonReadable, JsonWritable, LocalDateKeySerializer, LocalDateSerializer}
import org.json4s.FieldSerializer.{renameFrom, renameTo}
import org.json4s.{DefaultFormats, FieldSerializer}

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneOffset}

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
    val nonMetaPropChanges = changes.filter(_.property.propertyType!="meta")
    assert(nonMetaPropChanges.size == nonMetaPropChanges.map(_.property.name).toSet.size)
    nonMetaPropChanges.forall(c => c.currentValue.isEmpty || c.previousValue.isEmpty || c.currentValue.get!=c.previousValue.get)
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
    Instant.parse(str).atZone(ZoneOffset.UTC).toLocalDateTime
    //LocalDateTime.parse(str,InfoboxRevision.formatter)
  }


  override implicit def formats = InfoboxRevision.formats

  def validFromAsDate  = stringToDate(validFrom)
}

object InfoboxRevision extends JsonReadable[InfoboxRevision] with StrictLogging {

  val formatter: DateTimeFormatter = DateTimeFormatter.ISO_INSTANT//.ofPattern("MMM d[d], yyyy h[h]:mm:ss a")

  val rename1 = FieldSerializer[InfoboxRevision](renameTo("type", "revisionType"),renameFrom("type", "revisionType"))
  val rename2 = FieldSerializer[InfoboxProperty](renameTo("type", "propertyType"),renameFrom("type", "propertyType"))


  override implicit def formats = (DefaultFormats.preservingEmptyValues
    + LocalDateSerializer
    + LocalDateKeySerializer
    + rename1
    + rename2)

}
