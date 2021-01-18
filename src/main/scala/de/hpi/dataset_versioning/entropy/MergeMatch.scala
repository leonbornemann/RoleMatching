package de.hpi.dataset_versioning.entropy

import java.io.PrintWriter

case class MergeMatch(first: FieldLineageAsCharacterString, second: FieldLineageAsCharacterString) {

  val subdomain = "org.cityofchicago"

  val merged = first.mergeCompatible(second)
  val firstEntropy = first.defaultEntropy
  val secondEntropy = second.defaultEntropy
  val mergedEntropy = merged.defaultEntropy


  def entropyDifferenceBeweenOriginals = Math.abs(first.defaultEntropy - second.defaultEntropy)

  def makeLineageString(first: String) = "[" + first.mkString(",") + "]"

  // DIFFERENCE:${entropyDifferenceBeweenOriginals}%1.3f
  def printShort = println(f"DIFF: $entropyDifferenceBeweenOriginals%1.3f REDUCTION:$entropyReduction%1.3f\n" +
    f"${first.printWithEntropy}\n" +
    f"${second.printWithEntropy}\n") //(${entropy(elem)}%1.3f) MERGE  $s (${entropy(s)}%1.3f) TO $merged (${entropy(merged)}%1.3f)")

  def entropyReduction = firstEntropy + secondEntropy - mergedEntropy

  def exportActualTableMatch(targetPath:String) = {
    val pr = new PrintWriter(targetPath)
    new AssociationGraphExplorer().printInfoToFile(None,first.dttID(subdomain),second.dttID(subdomain),pr)
  }

}
