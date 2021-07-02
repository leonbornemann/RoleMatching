package de.hpi.socrata.json.custom_serializer

import de.hpi.socrata.io.Socrata_IOService
import org.json4s.CustomSerializer
import org.json4s.JsonAST.JString

import java.time.LocalDate

case object LocalDateSerializer
  extends CustomSerializer[LocalDate](
    format =>
      ( {
        case JString(s) => LocalDate.parse(s)
      }, {
        case d: LocalDate => JString(Socrata_IOService.dateTimeFormatter.format(d))
      })
  )
