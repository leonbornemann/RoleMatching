package de.hpi.oneshot

object GenerateDittoConfigJsonString extends App {

  val dsNames = Seq("austintexas",  "chicago"  ,"gov.maryland"  ,"oregon"  ,"utah")
  dsNames.foreach(dsname => {
    val configString = s"""{
                          |    "name": "$dsname",
                          |    "task_type": "classification",
                          |    "vocab": ["0", "1"],
                          |    "trainset": "../../data/dittoExport/socrata/$dsname.json.txt_train.txt",
                          |    "validset": "../../data/dittoExport/socrata/$dsname.json.txt_validation.txt",
                          |    "testset": "../../data/dittoExport/socrata/$dsname.json.txt_test.txt"
                          |  },""".stripMargin
    println(configString)
  })
}
