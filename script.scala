import sc._

val N = 10000000

// Generate a sequence of numbers and distribute
val par = parallelize(1 to N)

// Generate a point in 2D unit square
def randomPoint:(Double,Double) = {
    val x = Math.random()
    val y = Math.random()
    (x,y)
}

// Check if a point lies in the unit circle
def inCircle(point:(Double,Double)):Int = {
    if (point._1*point._1 + point._2*point._2 < 1) 1 else 0
}

// List of hits yes/no
val inCircleList = par map(i => inCircle(randomPoint))

// Return the first 5 elements from the RDD
inCircleList take 5

// Get info about the RDD
inCircleList.toDebugString

// The number of hits
val total = inCircleList reduce (_+_)

// Probability of hitting the circle *4 = Pi
val S = 4. * total / N


// -------------------------------------------------

val count = parallelize(1 to N).map{i =>
  val x = Math.random()
  val y = Math.random()
  if (x*x + y*y < 1) 1 else 0
}.reduce(_ + _)
println("Pi is roughly " + 4.0 * count / N)


// -------------------------------------------------

// Read a file, e.g. Ulysses from Project Gutenberg
// and process it similar to Hadoop M/R
val file = textFile("Joyce-Ulysses.txt")

// Convert to an array of words in the text
val words = file.flatMap(_.split(" "))

// Map to (key,value) pairs
val mapped = words map (word => (word,1))

// Sort and group by key, 
// Result is of form (key, List(value1, value2, value3, ...))
val grouped = mapped sortByKey() groupByKey()

// The length of the values array yields the amount
val result = grouped map {case (k,vs) => (k,vs.length)}
// But where is the *reduce*?

result.collect

// -------------------------------------------------

// The length of the values array yields the amount
// val result = grouped map {case (k,vs) => (k,vs.length)}
val result = grouped map {case (k,vs) => (k, vs reduce (_+_))}

// -------------------------------------------------

// In Spark, we would do something like this:
val file = textFile("Joyce-Ulysses.txt")
val words = file.flatMap(_.split(" "))
val mapped = words map (word => (word,1))
val result = mapped reduceByKey(_+_)
result collect

// -------------------------------------------------

// Caching !!!

val file = textFile("Joyce-Ulysses.txt")
val words = file.flatMap(_.split(" "))
val mapped = words map (word => (word,1))
// Cache the RDD for later use
val cached = mapped cache()
// Use the cached version
val result = cached reduceByKey(_+_)
// Oops, nothing happens?
result.collect
// Laziness... oh my
result.collect

// Count how many times the word 'the' occurs in the text
cached filter {case(word,v) => word=="the"} reduceByKey(_+_) collect

// What is the top-5 of occurrences?
result map{case(word,v) => (v,word)} sortByKey(false) map{case(v,word) => (word,v)} take 5


// -------------------------------------------------

// Load files from HDFS
val covFile = sc.textFile("NA12878.chrom19.SLX.maq.SRP000032.2009_07.coverage",8)
val bedFile = sc.textFile("201101_encode_motifs_in_tf_peaks.bed",8)

// Class to hold records from coverage data
class covData(val chr: String, val pos: Int, val cov: Int) {
    def this(line: Array[String]) {
     this(line(0).toString, line(1).toInt, line(2).toInt)
    }
}

// Class to hold records from Transcription Factor data
class tfsData(val chr: String, val pos1: Int, val pos2:Int, val tf: String) {
    def this(line: Array[String]) {
     this(line(0).toString, line(1).toInt, line(2).toInt, line(3).toString)
    }
}

// Turn input files into an RDD of objects
val cov = covFile.map(_.split("\\s+")).map(new covData(_))
val tfs = bedFile.map(_.split("\\s+")).map(new tfsData(_))

// Count the number of items in both datasets
cov.count
tfs.count

// Cache in memory
val ccov = cov cache
val ctfs = tfs cache

// Count once for the caching to occur
ccov.count
ctfs.count

// Turn coverage data into K/V pairs
val kvcov = ccov.map(x => (x.pos,(x.cov))).cache
// Turn TF data into K/V pairs
val kvtfs = ctfs.filter(x => x.chr == "chr19").map(x => (x.pos1,(x.pos2,x.tf)))

// Activate the caching of the coverage data
kvcov.count

// Join both datasets together by key
val cjoined = kvcov.join(kvtfs)

// Waaaw, that's fast! In fact, nothing happened yet.
// select 5 entries to see the result but reformat first
val flatjoined = cjoined map { case(x,(y,(z,zz))) => (x,z,zz,y) }
flatjoined take 5

// ---------------------------









