package de.hpi.dataset_versioning.db_synthesis.sketches

import java.io.{File, FileOutputStream, ObjectOutputStream}

trait BinarySerializable extends Serializable{

  private def serialVersionUID = 6529685098267757690L

  def writeToBinaryFile(f:File) = {
    val o = new ObjectOutputStream(new FileOutputStream(f))
    o.writeObject(this)
    o.close()
  }
}
