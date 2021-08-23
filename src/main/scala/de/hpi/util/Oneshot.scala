package de.hpi.util

import java.time.{Duration, LocalDate, Period}

object Oneshot extends App {
  val datesSocrata = Seq(
    "2020-04-30",
    "2020-05-31",
    "2020-06-30",
    "2020-07-31",
    "2020-08-31",
    "2020-09-30"
  ).map(LocalDate.parse(_))
  val datesWikipedia = Seq(
    "2011-05-07",
    "2012-05-05",
    "2013-05-04",
    "2014-05-03",
    "2015-05-02",
    "2016-04-30",
    "2017-04-29",
    "2018-04-28",
  ).map(LocalDate.parse(_))
  val startDateSocrata = LocalDate.parse("2019-11-01")
  val endDateSocrata = LocalDate.parse("2020-11-01")
  val startDateWikipedia=LocalDate.parse("2003-01-04")
  val endDateWikipedia=LocalDate.parse("2019-09-07")

  val totalDuration = endDateWikipedia.toEpochDay - startDateWikipedia.toEpochDay

  datesWikipedia
    .map(d => {
      val duration = d.toEpochDay - startDateWikipedia.toEpochDay
      val percentage:Double = 100* duration / totalDuration.toDouble
      (d,Math.round(percentage))
    })
    .foreach(p => println(s"'${p._1}' : ${"%d".format(p._2)},"))

}
