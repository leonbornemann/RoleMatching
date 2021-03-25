package de.hpi.dataset_versioning.db_synthesis.sketches.field

import java.time.LocalDate

case class ChangePoint[A](prevValueA:A, prevValueB:A, curValueA:A, curValueB:A, pointInTime:LocalDate, prevPointInTime:LocalDate) {

}
