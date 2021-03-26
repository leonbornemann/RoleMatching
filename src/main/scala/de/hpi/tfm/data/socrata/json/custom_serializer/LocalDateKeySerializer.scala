package de.hpi.tfm.data.socrata.json.custom_serializer

import de.hpi.tfm.io.IOService
import org.json4s.CustomKeySerializer

import java.time.LocalDate

case object LocalDateKeySerializer extends CustomKeySerializer[LocalDate](format => ( {
  case s: String => LocalDate.parse(s, IOService.dateTimeFormatter)
}, {
  case date: LocalDate => date.format(IOService.dateTimeFormatter)
}
))
