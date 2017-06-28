//Let's caluculate pi with Monte Carlo estimation
import scala.math.random

//make a very large unique set of 1 -> n 
val partitions = 2 
val n = math.min(100000L * partitions, Int.MaxValue).toInt 
val xs = 1 until n 

//split up n into tht number of partitions we can use 
val rdd = sc.parallelize(xs, partitions).setName("'N values rdd'")

//generate a random set of points within a 2x2 square
val sample = rdd.map { i =>
  val x = random * 2 - 1
  val y = random * 2 - 1
  (x, y)
}.setName("'Random points rdd'")

//points w/in the square also w/in the center circle of r=1
val inside = sample.filter { case (x, y) => (x * x + y * y < 1) }.setName("'Random points inside circle'")
val count = inside.count()
 
//Area(circle)/Area(square) = inside/n => pi=4*inside/n                        
println("Pi is roughly " + 4.0 * count / n)
