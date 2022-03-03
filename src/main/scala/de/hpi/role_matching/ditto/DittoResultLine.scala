package de.hpi.role_matching.ditto

import de.hpi.role_matching.playground.{JsonReadable, JsonWritable}
import org.json4s.FieldSerializer
import org.json4s.FieldSerializer.{renameFrom, renameTo}

case class DittoResultLine(left:String,right:String,_match:Int,match_confidence:Double) extends JsonWritable[DittoResultLine]{

  override implicit def formats = super.formats + FieldSerializer[Int](
    renameTo("_match", "match"),
    renameFrom("match","_match")
  )

}
object DittoResultLine extends JsonReadable[DittoResultLine] {

  override implicit def formats = super.formats + FieldSerializer[DittoResultLine](
    renameTo("_match", "match"),
    renameFrom("match","_match")
  )
}
