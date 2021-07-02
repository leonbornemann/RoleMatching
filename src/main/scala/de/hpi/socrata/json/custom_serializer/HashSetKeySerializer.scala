package de.hpi.socrata.json.custom_serializer

import org.json4s.CustomKeySerializer

case object HashSetKeySerializer extends CustomKeySerializer[Set[Int]](format => ( {
  case s: String => s.substring(0, s.size - 1).split(",").map(_.toInt).toSet
}, {
  case set: Set[Int] => s"{${set.mkString(",")}}"
}
))
