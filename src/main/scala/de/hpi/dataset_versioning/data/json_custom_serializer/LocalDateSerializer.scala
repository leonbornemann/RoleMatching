package de.hpi.dataset_versioning.data.json_custom_serializer

import java.time.LocalDate

import de.hpi.dataset_versioning.io.IOService
import org.json4s.CustomSerializer
import org.json4s.JsonAST.JString

case object LocalDateSerializer
  extends CustomSerializer[LocalDate](
    format =>
      ({
        case JString(s) => LocalDate.parse(s)
      }, {
        case d:LocalDate => JString(IOService.dateTimeFormatter.format(d))
      })
  )
