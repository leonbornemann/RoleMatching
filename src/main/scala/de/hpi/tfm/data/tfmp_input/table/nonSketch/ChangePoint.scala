package de.hpi.tfm.data.tfmp_input.table.nonSketch

import java.time.LocalDate

case class ChangePoint[A](prevValueA: A, prevValueB: A, curValueA: A, curValueB: A, pointInTime: LocalDate, prevPointInTime: LocalDate) {

}
