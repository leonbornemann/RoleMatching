package de.hpi.tfm.data.wikipedia.infobox

import de.hpi.tfm.data.socrata.{JsonReadable, JsonWritable}

case class InfoboxRevision(revisionId:BigInt,
                           template:Option[String]=None,
                           pageTitle:String,
                           changes:IndexedSeq[InfoboxChange],
                           attributes:Any,
                           validFrom:String,
                           position:Option[Int]=None,
                           pageID:BigInt,
                           //type1:String,
                           user:Option[User]=None,
                           key:String,
                           validTo:Option[String]=None
) extends JsonWritable[InfoboxRevision]

object InfoboxRevision extends JsonReadable[InfoboxRevision]
