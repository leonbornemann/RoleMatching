package de.hpi.dataset_versioning.db_synthesis.sketches

import java.io.{File, FileInputStream, ObjectInputStream}

trait BinaryReadable[A] {

  def loadFromFile(f:File) = {
    val oi = new ObjectInputStream(new FileInputStream(f))
    val sketch = oi.readObject().asInstanceOf[A]
    oi.close()
    sketch
  }

}
