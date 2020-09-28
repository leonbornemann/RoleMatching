package de.hpi.dataset_versioning.db_synthesis.sketches.table

import java.io._

import de.hpi.dataset_versioning.db_synthesis.baseline.database.AbstractTemporalDatabaseTable
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier
import de.hpi.dataset_versioning.db_synthesis.sketches.column.TemporalColumnSketch

import scala.collection.mutable

@SerialVersionUID(3L)
abstract class TemporalTableSketch(unionedTables:mutable.HashSet[DecomposedTemporalTableIdentifier],
                          val temporalColumnSketches:Array[TemporalColumnSketch]) extends AbstractTemporalDatabaseTable[Int](unionedTables) with Serializable{

  def tableActiveTimes = temporalColumnSketches.map(_.attributeLineage.activeTimeIntervals).reduce((a,b) => a.union(b))

  def writeToBinaryFile(f:File) = {
    val o = new ObjectOutputStream(new FileOutputStream(f))
    o.writeObject(this)
    o.close()
  }
}
object TemporalTableSketch {

  def loadFromFile[A](f:File) = {
    println(f.getAbsolutePath)
    val oi = new ObjectInputStream(new FileInputStream(f))
    val sketch = oi.readObject().asInstanceOf[A]
    oi.close()
    sketch
  }
}

