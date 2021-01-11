package de.hpi.dataset_versioning.oneshot

import de.hpi.dataset_versioning.oneshot.EnthropyShenanigansMain.{entropyV2, mergeCompatible}

case class MergeMatch(first: String, second: String) {
  val entropyFunction = (s: String) => EnthropyShenanigansMain.entropyV2(s)

  val merged = EnthropyShenanigansMain.mergeCompatible(first,second)
  val firstEntropy = entropyFunction(first)
  val secondEntropy = entropyFunction(second)
  val mergedEntropy = entropyFunction(merged)


  def entropyDifferenceBeweenOriginals = Math.abs(entropyFunction(first) - entropyFunction(second))

  def makeLineageString(first: String) = "[" + first.mkString(",") + "]"

  // DIFFERENCE:${entropyDifferenceBeweenOriginals}%1.3f
  def printShort = println(f"Entropy Merge after GAIN:$entropyReduction%1.3f)\n" +
    f"#1:${makeLineageString(first)}\n" +
    f"#2:${makeLineageString(second)}\n")//(${entropy(elem)}%1.3f) MERGE  $s (${entropy(s)}%1.3f) TO $merged (${entropy(merged)}%1.3f)")

  def entropyReduction = firstEntropy+secondEntropy-mergedEntropy

}
