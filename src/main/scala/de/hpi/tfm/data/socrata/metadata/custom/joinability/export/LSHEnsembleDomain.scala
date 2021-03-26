package de.hpi.tfm.data.socrata.metadata.custom.joinability.`export`

import de.hpi.tfm.data.socrata.JsonWritable

case class LSHEnsembleDomain(id: String, version: String, attrName: String, values: Set[String]) extends JsonWritable[LSHEnsembleDomain]{

}
