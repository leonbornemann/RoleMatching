package de.hpi.oneshot

object GenerateDittoConfigJsonString extends App {

  //socrata
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
  //wikipedia: education.json  football.json  military.json  politics.json  tv_and_film.json
  val dsNamesWikipedia = Seq("education","military"  ,"politics"  ,"tv_and_film","football")
  val configNames = Seq("NO_DECAY_7_2016-05-07", "PROBABILISTIC_DECAY_FUNCTION_0.3_7_2016-05-07", "PROBABILISTIC_DECAY_FUNCTION_0.7_7_2016-05-07")
  configNames.foreach(config => {
    dsNamesWikipedia.foreach(dsname => {
      val configString = s"""{
                            |    "name": "${dsname}_$config",
                            |    "task_type": "classification",
                            |    "vocab": ["0", "1"],
                            |    "trainset": "../../data/dittoExport/wikipedia/$config/$dsname.txt_train.txt",
                            |    "validset": "../../data/dittoExport/wikipedia/$config/$dsname.txt_validation.txt",
                            |    "testset": "../../data/dittoExport/wikipedia/$config/$dsname.txt_test.txt"
                            |  },""".stripMargin
      println(configString)
    })
  })

}
