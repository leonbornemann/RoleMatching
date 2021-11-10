package de.hpi.role_matching.playground

case class TableHistory(pageID:BigInt,
                        pageTitle:String,
                        tableID:String,
                        tables:IndexedSeq[TableVersion]) extends JsonReadable[TableHistory]

object TableHistory extends JsonWritable[TableHistory] {

}
