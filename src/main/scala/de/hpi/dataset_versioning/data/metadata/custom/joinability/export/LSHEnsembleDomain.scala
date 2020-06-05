package de.hpi.dataset_versioning.data.metadata.custom.joinability.`export`

import de.hpi.dataset_versioning.data.JsonWritable

case class LSHEnsembleDomain(id: String, version: String, attrName: String, values: Set[String]) extends JsonWritable[LSHEnsembleDomain]{

}
