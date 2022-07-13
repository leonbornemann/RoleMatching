package de.hpi.role_matching.evaluation.matching

import de.hpi.role_matching.playground.{JsonReadable, JsonWritable}
import org.json4s.FieldSerializer
import org.json4s.FieldSerializer.{renameFrom, renameTo}

import java.io.PrintWriter

case class DittoResult(left:String,right:String,_match:Int,match_confidence:Double) extends JsonWritable[DittoResult]{

  def normalizedScore = if(_match==1) match_confidence else 1.0 -match_confidence

  def toCSVLine(isTrueMatch:Boolean) = s"$isTrueMatch,${_match==1},$match_confidence,$normalizedScore"

  override implicit def formats = super.formats + FieldSerializer[Int](
    renameTo("_match", "match"),
    renameFrom("match","_match")
  )
}
object DittoResult extends JsonReadable[DittoResult] {


  override implicit def formats = super.formats + FieldSerializer[DittoResult](
    renameTo("_match", "match"),
    renameFrom("match","_match")
  )

  def appendSchema(resultPR: PrintWriter) = resultPR.println("isTrueMatch,isPredictedMatch,matchConfidence,normalizedScore")

}
