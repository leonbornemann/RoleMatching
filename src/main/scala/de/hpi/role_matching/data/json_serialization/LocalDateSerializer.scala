package de.hpi.role_matching.data.json_serialization

import de.hpi.util.GLOBAL_CONFIG
import org.json4s.CustomSerializer
import org.json4s.JsonAST.JString

import java.time.LocalDate

case object LocalDateSerializer
  extends CustomSerializer[LocalDate](
    format =>
      ( {
        case JString(s) => LocalDate.parse(s)
      }, {
        case d: LocalDate => JString(GLOBAL_CONFIG.dateTimeFormatter.format(d))
      })
  )
