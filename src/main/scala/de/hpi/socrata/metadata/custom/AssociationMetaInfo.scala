package de.hpi.socrata.metadata.custom

import de.hpi.socrata.tfmp_input.association.AssociationIdentifier

case class AssociationMetaInfo(id:AssociationIdentifier, colName:String, attrID:Int, cardinality:Int) {

}
