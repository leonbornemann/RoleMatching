package de.hpi.dataset_versioning.db_synthesis.decomposition.normalization

import java.time.LocalDate

import de.hpi.dataset_versioning.data.simplified.Attribute

case class FDBasedNormalizationResult(id:String, version:LocalDate, originalAttributes:IndexedSeq[Attribute], decomposedTables:IndexedSeq[DecomposedTable])
