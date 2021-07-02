package de.hpi.socrata.json.custom_serializer

import de.hpi.socrata.io.Socrata_IOService
import org.json4s.CustomKeySerializer

import java.time.LocalDate

case object LocalDateKeySerializer extends CustomKeySerializer[LocalDate](format => ( {
  case s: String => LocalDate.parse(s, Socrata_IOService.dateTimeFormatter)
}, {
  case date: LocalDate => date.format(Socrata_IOService.dateTimeFormatter)
}
))
