package de.hpi.tfm.data.wikipedia.infobox

object MainClass extends App {
  val file = "/home/leon/data/dataset_versioning/WIkipedia/infoboxes/sample/enwiki-20171103-pages-meta-history10.xml-p2336425p2370393.output.json"
  val objects = InfoboxRevision.fromJsonObjectPerLineFile(file)
  InfoboxRevision.toChangeCube(objects)
  println()
}
