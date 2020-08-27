package de.hpi.dataset_versioning.db_synthesis.sketches

import java.io.{File, FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}

class TemporalTableSketch(val temporalColumnSketches:Array[TemporalColumnSketch]) extends Serializable{

  private def serialVersionUID = 6529685098267757623L

  def tableActiveTimes = temporalColumnSketches.map(_.attributeLineage.activeTimeIntervals).reduce((a,b) => a.union(b))

  def writeToBinaryFile(f:File) = {
    val o = new ObjectOutputStream(new FileOutputStream(f))
    o.writeObject(this)
    o.close()
  }
}
object TemporalTableSketch {

  def loadFromFile[A](f:File) = {
    val oi = new ObjectInputStream(new FileInputStream(f))
    val sketch = oi.readObject().asInstanceOf[A]
    oi.close()
    sketch
  }
}

