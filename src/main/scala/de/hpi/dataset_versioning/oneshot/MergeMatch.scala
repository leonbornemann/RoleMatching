package de.hpi.dataset_versioning.oneshot

import de.hpi.dataset_versioning.oneshot.EntropyShenanigansMain.{entropyV2, mergeCompatible}

case class MergeMatch(first: String, second: String,label1:String,label2:String) {
  val entropyFunction = (s: String) => EntropyShenanigansMain.entropyV2(s)

  val merged = EntropyShenanigansMain.mergeCompatible(first,second)
  val firstEntropy = entropyFunction(first)
  val secondEntropy = entropyFunction(second)
  val mergedEntropy = entropyFunction(merged)


  def entropyDifferenceBeweenOriginals = Math.abs(entropyFunction(first) - entropyFunction(second))

  def makeLineageString(first: String) = "[" + first.mkString(",") + "]"

  // DIFFERENCE:${entropyDifferenceBeweenOriginals}%1.3f
  def printShort = println(f"DIFF: $entropyDifferenceBeweenOriginals%1.3f REDUCTION:$entropyReduction%1.3f\n" +
    f"$label1:${makeLineageString(first)}\n" +
    f"$label2:${makeLineageString(second)}\n")//(${entropy(elem)}%1.3f) MERGE  $s (${entropy(s)}%1.3f) TO $merged (${entropy(merged)}%1.3f)")

  def entropyReduction = firstEntropy+secondEntropy-mergedEntropy

}
