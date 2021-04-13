package de.hpi.tfm.data.wikipedia.infobox

import de.hpi.tfm.data.socrata.json.custom_serializer.{LocalDateKeySerializer, LocalDateSerializer}
import de.hpi.tfm.data.socrata.metadata.Provenance
import de.hpi.tfm.data.socrata.{JsonReadable, JsonWritable}
import de.hpi.tfm.data.wikipedia.infobox.InfoboxRevision.rename
import org.json4s.{DefaultFormats, FieldSerializer}
import org.json4s.FieldSerializer.{renameFrom, renameTo}
import org.json4s.ext.EnumNameSerializer

case class InfoboxRevision(revisionId:BigInt,
                           template:Option[String]=None,
                           pageTitle:String,
                           changes:IndexedSeq[InfoboxChange],
                           attributes:Option[Map[String,String]],
                           validFrom:String,
                           position:Option[Int]=None,
                           pageID:BigInt,
                           revisionType:String,
                           user:Option[User]=None,
                           key:String,
                           validTo:Option[String]=None
) extends JsonWritable[InfoboxRevision]{

  def checkIntegrity() ={
    assert(revisionType=="DELETE" ^ attributes.isDefined) //^ = xor
    changes.forall(c => c.currentValue.isEmpty || c.previousValue.isEmpty || c.currentValue.get!=c.previousValue.get)
    if(revisionType=="DELETE")
      assert(changes.forall(_.currentValue.isEmpty))
    if(revisionType=="CREATE")
      assert(changes.forall(_.previousValue.isEmpty))
  }


  override implicit def formats = InfoboxRevision.formats

}

object InfoboxRevision extends JsonReadable[InfoboxRevision] {

  val rename = FieldSerializer[InfoboxRevision](renameTo("type", "revisionType"),renameFrom("type", "revisionType"))

  override implicit def formats = (DefaultFormats.preservingEmptyValues
    + LocalDateSerializer
    + LocalDateKeySerializer
    + rename)

  def toChangeCube(objects: collection.Seq[InfoboxRevision]) = {
    val byKey = objects.groupBy(_.key)
    byKey.flatMap(k => InfoboxRevisionHistory(k._1,k._2).toChangeCube)
  }

}
