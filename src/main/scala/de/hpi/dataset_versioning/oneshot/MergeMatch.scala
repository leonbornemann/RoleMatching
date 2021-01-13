package de.hpi.dataset_versioning.oneshot

import de.hpi.dataset_versioning.oneshot.EntropyShenanigansMain.{FieldLineage}

case class MergeMatch(first: FieldLineage, second: FieldLineage) {

  val merged = first.mergeCompatible(second)
  val firstEntropy = first.defaultEntropy
  val secondEntropy = second.defaultEntropy
  val mergedEntropy = merged.defaultEntropy


  def entropyDifferenceBeweenOriginals = Math.abs(first.defaultEntropy - second.defaultEntropy)

  def makeLineageString(first: String) = "[" + first.mkString(",") + "]"

  // DIFFERENCE:${entropyDifferenceBeweenOriginals}%1.3f
  def printShort = println(f"DIFF: $entropyDifferenceBeweenOriginals%1.3f REDUCTION:$entropyReduction%1.3f\n" +
    f"$first\n" +
    f"$second\n")//(${entropy(elem)}%1.3f) MERGE  $s (${entropy(s)}%1.3f) TO $merged (${entropy(merged)}%1.3f)")

  def entropyReduction = firstEntropy+secondEntropy-mergedEntropy

}
