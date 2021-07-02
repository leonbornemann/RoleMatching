package de.hpi.socrata.json.custom_serializer

import de.hpi
import de.hpi.socrata.DatasetInstance
import de.hpi.socrata.io.Socrata_IOService
import org.json4s.CustomKeySerializer

import java.time.LocalDate

case object DatasetInstanceKeySerializer extends CustomKeySerializer[DatasetInstance](format => ( {
  case s: String => hpi.socrata.DatasetInstance(s.split(",")(0), LocalDate.parse(s.split(",")(1), Socrata_IOService.dateTimeFormatter))
}, {
  case i: DatasetInstance => s"${i.id},${i.date.format(Socrata_IOService.dateTimeFormatter)}"
}
)) {

}
