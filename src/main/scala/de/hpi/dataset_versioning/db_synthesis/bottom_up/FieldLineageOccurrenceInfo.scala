package de.hpi.dataset_versioning.db_synthesis.bottom_up

import java.time.LocalDate

import de.hpi.dataset_versioning.data.{JsonReadable, JsonWritable}

import scala.collection.mutable

case class FieldLineageOccurrenceInfo(lineage: collection.IndexedSeq[(LocalDate, Any)], cols: Set[DatasetColumn],f:Field) extends JsonWritable[FieldLineageOccurrenceInfo]

object FieldLineageOccurrenceInfo extends JsonReadable[FieldLineageOccurrenceInfo]
