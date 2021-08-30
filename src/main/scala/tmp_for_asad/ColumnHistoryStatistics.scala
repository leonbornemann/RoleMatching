package tmp_for_asad

import org.joda.time.LocalDateTime

import java.time.LocalDate

case class ColumnHistoryStatistics(filename:String,
                                   tableID:String,
                                   columnID:String,
                                   humanReadableColumnName:String,
                                   versionTimestamps:List[LocalDateTime],
                                   percentageOfLifetimeNumeric:Double,
                                   percentageOfLifetimeHasHTMLHeaderInContent:Double,
                                  ) //
