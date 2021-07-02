package de.hpi.socrata.metadata.custom.joinability.`export`

import de.hpi.socrata.JsonWritable

case class LSHEnsembleDomain(id: String, version: String, attrName: String, values: Set[String]) extends JsonWritable[LSHEnsembleDomain]{

}
