package de.hpi.tfm.data.socrata.metadata.custom

import de.hpi.tfm.data.tfmp_input.association.AssociationIdentifier

case class AssociationMetaInfo(id:AssociationIdentifier, colName:String, attrID:Int, cardinality:Int) {

}
