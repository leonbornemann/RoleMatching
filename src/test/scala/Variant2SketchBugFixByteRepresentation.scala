import java.time.LocalDate

import de.hpi.dataset_versioning.db_synthesis.sketches.field.Variant2Sketch

import scala.collection.mutable

object Variant2SketchBugFixByteRepresentation extends App {
  val variant2Sketch = Variant2Sketch.fromTimestampToHash(mutable.TreeMap(LocalDate.parse("2019-11-01") -> 47899831))
  val a = variant2Sketch.valueAt(LocalDate.parse("2020-03-15"))
  println(a)


}
