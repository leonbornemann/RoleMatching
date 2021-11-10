package de.hpi.role_matching.playground

object Main extends App {
  //https://www.scala-lang.org/api/2.13.7/scala/collection/index.html
  //Variablen:
  val myStringVariable:String = "My String" //immutable
  var myMutableVariable:String = "test1"
  myMutableVariable = "tes2"
  //Datatype is not necessary:
  val myStringVal = "test"
  val myInt = 123
  val hmwhattypeisthis = myStringVal + myInt
  val whattypeisthat = 123 + 1.0
  //printing variables
  println(s"The value of whattypeisthat +10 is ${whattypeisthat + 10}")
  //immutable collections:
  val myList:Seq[Int] = Seq(1,2,3,4,5)
  val myNewList = myList.head
  println(myNewList)
  val myArrayList = IndexedSeq(1,2,3,4,5)
  println(myList ++ myArrayList)
  println(myList)
  println(myList(3))
  //mutable collections:
  val myMutableList = scala.collection.mutable.ArrayBuffer(1,2,3,4,5,6)
  myMutableList.append(7)
  println(myMutableList)
  //if-statements:
  if( myMutableList(1) > 7){
    //do something
  } else if(myMutableList(1)==7){
    //do something else
  } else {
    //do something else
  }
  //loops:
  var i=0
  while(i< myMutableList.size){
    println(myMutableList(i))
    i+=1
  }
  private val range: Range = 0 until myMutableList.size
  for (i <- range){ //entspricht for each schleife

  }
  //often you don't need loops!
  println("-----------------------------------------------------------------------------------------------------")
  println("------------------------Functional Programming instead of loops--------------------------------------")
  myMutableList.foreach(printInt) //with explicit function definition

  def printInt(i:Int):Unit = {
    println("This integer value is " + i)
  }

  myMutableList.foreach((i:Int) => println("This Integer value is " + i)) // with anonymous function
  myMutableList.foreach(i => {
    println("This Integer value is " + i)
  }) // without type

  //map function creates a new collection  where every element of the old collection is transformed to a new object (1-to-1)
  val listPlus100 = myMutableList.map(i => i+100.0)
  println(listPlus100)
  //filter creates a new collection that retains all elements that satisfy a boolean condition
  val filtered = myMutableList.filter(i => i>5)
  println(filtered)
  // summarizing collections to one element:
  filtered.sum
  filtered.size
  //flatmap
  val myStringList = Seq("This is a sentence.","This is another sentence.","I am not very creative.")
  val words = myStringList.flatMap(string => string.split("\\s"))
  println(words)
  val wordsAsListofLists = myStringList.map(string => string.split("\\s"))

}
