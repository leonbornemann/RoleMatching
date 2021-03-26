package de.hpi.tfm.data.socrata.json.custom_serializer

import de.hpi.tfm.data.socrata
import de.hpi.tfm.data.socrata.DatasetInstance
import de.hpi.tfm.io.IOService
import org.json4s.CustomKeySerializer

import java.time.LocalDate

case object DatasetInstanceKeySerializer extends CustomKeySerializer[DatasetInstance](format => ( {
  case s: String => socrata.DatasetInstance(s.split(",")(0), LocalDate.parse(s.split(",")(1), IOService.dateTimeFormatter))
}, {
  case i: DatasetInstance => s"${i.id},${i.date.format(IOService.dateTimeFormatter)}"
}
)) {

}
