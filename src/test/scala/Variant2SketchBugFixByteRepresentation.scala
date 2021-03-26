

import de.hpi.tfm.data.tfmp_input.table.sketch.FactLineageSketch

import java.time.LocalDate
import scala.collection.mutable

object Variant2SketchBugFixByteRepresentation extends App {
  val variant2Sketch = FactLineageSketch.fromTimestampToHash(mutable.TreeMap(LocalDate.parse("2019-11-01") -> 47899831))
  val a = variant2Sketch.valueAt(LocalDate.parse("2020-03-15"))
  println(a)


}
