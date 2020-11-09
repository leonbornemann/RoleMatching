package de.hpi.dataset_versioning.db_synthesis.database.table

@SerialVersionUID(3L)
class BCNFSurrogateReferenceRow(var primaryKey:scala.collection.mutable.IndexedSeq[Int],val associationReferences:scala.collection.mutable.IndexedSeq[Int],foreignKeys:IndexedSeq[Int]) {

}
