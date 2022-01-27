package de.hpi.role_matching.cbrm.data.json_serialization

import de.hpi.role_matching.GLOBAL_CONFIG
import org.json4s.CustomKeySerializer

import java.time.LocalDate

case object LocalDateKeySerializer extends CustomKeySerializer[LocalDate](format => ( {
  case s: String => LocalDate.parse(s, GLOBAL_CONFIG.dateTimeFormatter)
}, {
  case date: LocalDate => date.format(GLOBAL_CONFIG.dateTimeFormatter)
}
))
