package de.hpi.dataset_versioning.data.json.custom_serializer

import de.hpi.dataset_versioning.data
import de.hpi.dataset_versioning.data.DatasetInstance
import de.hpi.dataset_versioning.io.IOService
import org.json4s.CustomKeySerializer

import java.time.LocalDate

case object DatasetInstanceKeySerializer extends CustomKeySerializer[DatasetInstance](format => ( {
  case s: String => data.DatasetInstance(s.split(",")(0), LocalDate.parse(s.split(",")(1), IOService.dateTimeFormatter))
}, {
  case i: DatasetInstance => s"${i.id},${i.date.format(IOService.dateTimeFormatter)}"
}
)) {

}
