package de.hpi.dataset_versioning.data.simplified

import com.google.gson.JsonElement

case class RelationalDatasetRow(id:Long = -1, var fields:collection.IndexedSeq[Any]) {

  def reorderColumns(oldToNewOrder: Map[Int, Int]) = {
    fields = fields.zipWithIndex
      .map{case (f,oldPosition) => (f,oldToNewOrder(oldPosition))}
      .sortBy(_._2)
      .map(_._1)
  }


  /***
   *
   * @return the fields of this collection with array being converted to a string
   */
  def arraysToString():collection.Seq[Any] = arraysToStringParam(fields)

  private def arraysToStringParam(seq:collection.Seq[Any]):collection.Seq[Any] = seq.map( f => {
    if(f.isInstanceOf[collection.Seq[Any]]){
      "[" +arraysToStringParam(f.asInstanceOf[collection.Seq[Any]]).mkString(",") + "]"
    } else{
      f
    }
  })

}
