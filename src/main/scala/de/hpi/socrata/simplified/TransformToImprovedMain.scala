package de.hpi.socrata.simplified

import de.hpi.socrata.io.Socrata_IOService

import java.time.LocalDate

object TransformToImprovedMain extends App {
  Socrata_IOService.socrataDir = args(0)
  val id = args(1)
  //val fromErrorFile = args.length >2 && args(2).toBoolean
  val timeRange = if(args.length > 2) Some(LocalDate.parse(args(2)),LocalDate.parse(args(3))) else None
  val transformer = new Transformer()
  if(id=="all")
    transformer.transformAll(timeRange)
  else
    transformer.transformAllForID(id,timeRange)

}
