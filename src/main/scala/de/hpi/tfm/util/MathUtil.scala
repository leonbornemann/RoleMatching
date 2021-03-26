package de.hpi.tfm.util

object MathUtil {

  //source: https://stackoverflow.com/questions/11581175/how-to-generate-the-power-set-of-a-set-in-scala
  def powerset[A](s: List[A]): List[List[A]] = {
     @annotation.tailrec
     def pwr(s: List[A], acc: List[List[A]]): List[List[A]] = s match {
       case Nil     => acc
           case a :: as => pwr(as, acc ::: (acc map (a :: _)))
         }
     pwr(s, Nil :: Nil)
   }


  def log2(a: Double) = math.log(a) / math.log(2)

}
