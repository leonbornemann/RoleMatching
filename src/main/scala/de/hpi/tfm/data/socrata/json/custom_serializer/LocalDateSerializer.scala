package de.hpi.tfm.data.socrata.json.custom_serializer

import de.hpi.tfm.io.IOService
import org.json4s.CustomSerializer
import org.json4s.JsonAST.JString

import java.time.LocalDate

case object LocalDateSerializer
  extends CustomSerializer[LocalDate](
    format =>
      ( {
        case JString(s) => LocalDate.parse(s)
      }, {
        case d: LocalDate => JString(IOService.dateTimeFormatter.format(d))
      })
  )
