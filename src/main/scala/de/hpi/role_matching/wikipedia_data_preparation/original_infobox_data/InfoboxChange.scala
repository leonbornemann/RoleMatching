package de.hpi.role_matching.wikipedia_data_preparation.original_infobox_data

//{"valueValidTo":"Jan 24, 2017 3:08:48 AM","property":"lats","currentValue":"10"}
case class InfoboxChange(valueValidTo: Option[String] = None,
                         property: InfoboxProperty,
                         currentValue: Option[String] = None,
                         previousValue: Option[String] = None) {

}
