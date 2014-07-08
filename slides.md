% Spark
% T. Verbeiren
% 9/7/2014


# Contents

Introduction

Spark

Examples

Ecosystem

Conclusions

- - -

# Me, Myself and I

- - -

# Introduction

- - - 

## Map / Reduce

\ 

### Mapper

\ 

### Reducer

\ 

(Nothing special)

- - -

## An experiment

- - -

## What if ...

- - - 

### ... we could just write

\ 

```scala
val y = x map () reduce ()
```

- - -

### ... this could be extended

\ 

```scala
val y = x map () filter() map () flatMap () reduce ()
```

- - -

## What would be needed?

\ 

1. Language support

<!-- Functions as first-class citizens -->

\ 

2. Platform

<!-- Fast and efficient resource management -->

\ 

3. Parallel abstraction mechanism

<!-- Some kind of abstraction that allows us to deal with what instead of how -->

- - -

# Spark

- - -

## 3 languages

\

Scala

Java

Python

- - -

## Platform

\ 

Built for low-latency

- - -

## Abststraction mechanism

\ 

**R**esilient **D**istributed **D**atasets

- - -

### RDDs

\ 

*Immutable* Collection

\ 

Accepting **transformations** and **actions**

- - -

### Transformations

\ 

- `map`
- `filter`
- `sample`
- `union` / `intersection`
- `groupByKey`
- `reduceByKey`
- `join`
- ...

- - -

### Actions

\ 

- `reduce`
- `collect`
- `count`
- `take(n)`
- `saveAsTextFile`
- ...

- - -

```scala
import sc._

val N = 10000000

val par = parallelize(1 to N)
// par: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[1] 
//   at parallelize at <console>:12
```

- - -

```scala
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
```

- - -

```
// List of hits yes/no
val inCircleList = par map(i => inCircle(randomPoint))

// Return the first 5 elements from the RDD
inCircleList take 5

// Get info about the RDD
inCircleList.toDebugString

// The number of hits
val total = inCircleList reduce (_+_)

// Probability of hitting the circle *4 = Pi
val P = 4. * total / N
```

- - -

From the [Spark examples](https://spark.apache.org/examples.html) page

\

```scala
val count = parallelize(1 to N).map{i =>
  val x = Math.random()
  val y = Math.random()
  if (x*x + y*y < 1) 1 else 0
}.reduce(_ + _)
println("Pi is roughly " + 4.0 * count / N)
```

- - -

Hadoop M/R in Spark

\ 

```scala
// Read a file, e.g. Ulysses from Project Gutenberg
// and process it similar to Hadoop M/R
val file = textFile("Joyce-Ulysses.txt")

// Retrieve an Array of words in the text
val words = file.flatMap(_.split(" "))

// Map to (key,value) pairs
val mapped = words map (word => (word,1)) 

// Sort and group by key, 
// Result is of form (key, List(value1, value2, value3, ...))
val grouped = mapped sortByKey() groupByKey()

// The length of the values array yields the amount
val result = grouped map {case (k,vs) => (k,vs.length)}
// But where is the *reduce*?
```

- - -

Be careful with _definitions_ of `map` and `reduce`!

\ 

```scala
// Read a file, e.g. Ulysses from Project Gutenberg
// and process it similar to Hadoop M/R
val file = textFile("Joyce-Ulysses.txt")

// Retrieve an Array of words in the text
val words = file.flatMap(_.split(" "))

// Map to (key,value) pairs
val mapped = words map (word => (word,1)) 

// Sort and group by key, 
// Result is of form (key, List(value1, value2, value3, ...))
val grouped = mapped sortByKey() groupByKey()

// The length of the values array yields the amount
// val result = grouped map {case (k,vs) => (k,vs.length)}
val result = grouped map {case (k,vs) => (k, vs reduce (_+_))}
```

- - - 

In Spark, we would use:

\ 

```scala
val file = sc.textFile("Joyce-Ulysses.txt")
val words = file.flatMap(_.split(" "))
val mapped = words map (word => (word,1))
val result = mapped reduceByKey(_+_)
```

- - -

## ... and REPL

- - -

## ... and Web interface

- - -

![](pics/SparkInterface.png)

- - -

## ... and distributed memory caching

- - -

**TODO**

```scala
val file = sc.textFile("Joyce-Ulysses.txt")
val words = file.flatMap(_.split(" "))
val mapped = words map (word => (word,1))
val result = mapped reduceByKey(_+_)
```

- - -

# Some more examples

- - -

## Coverage and transcription factors 

\ 

TODO

- - -

```scala
// Load files from HDFS
val covFile = sc.textFile("NA12878.chrom19.SLX.maq.SRP000032.2009_07.coverage",8)
val bedFile = sc.textFile("201101_encode_motifs_in_tf_peaks.bed",8)

class covData(val chr: String, val pos: Int, val cov: Int) {
    def this(line: Array[String]) {
     this(line(0).toString, line(1).toInt, line(2).toInt)
    }
}

class tfsData(val chr: String, val pos1: Int, val pos2:Int, val tf: String) {
    def this(line: Array[String]) {
     this(line(0).toString, line(1).toInt, line(2).toInt, line(3).toString)
    }
}

// Turn them into an RDD of objects
val cov = covFile.map(_.split("\\s+")).map(new covData(_))
val tfs = bedFile.map(_.split("\\s+")).map(new tfsData(_))

// Count the number of items in both datasets
cov.count
tfs.count

// Cache
val ccov = cov cache
val ctfs = tfs cache

// Count again
ccov.count

// This again takes a lot of time!?! Not this time!
ccov.count

// The same for ctfs
ctfs.count
ctfs.count

//ctfs take 5
val kvcov = ccov.map(x => (x.pos,(x.cov))).cache
val kvtfs = ctfs.filter(x => x.chr == "chr19").map(x => (x.pos1,(x.pos2,x.tf)))

// Cache the coverage data, collect the rest
kvcov.count
val tfs = kvtfs map {case(x,(y,z))=> (x,y,z)} collect 

val cjoined = kvcov.join(kvtfs)

// Waaaw, that's fast! In fact, nothing happened yet.
// select 5 entries:
val flatjoined = cjoined map { case(x,(y,(z,zz))) => (x,z,zz,y) }

flatjoined take 5
```

- - -

## Visualization of genomic data

\ 

TODO

- - -

# Ecosystem

- Spark SQL
- Spark Streaming
- Shark
- MLlib
- GraphX

- - -

# The end

\ 

Some links:

- [Slide sources](https://github.com/tverbeiren/BigDataBe-Spark)
- [Spark](https://spark.apache.org/)
- [Data Visualization Lab](http://datavislab.org)
- [ExaScience Life Lab](http://www.exascience.com/)
- [Data Intuitive](http://data-intuitive.com)


- - -

- - -

# Reference material and style info...

- - -

<style>
#barchart svg {
    overflow: hidden;
}

.node rect {
    stroke: #333;
    stroke-width: 1.5px;
    fill: #fff;
}

text {
  font-weight: 300;
  font-size: 20px;
}

.edgeLabel rect {
    fill: none;
    min-width: 60px;
}

.edgePath {
    stroke: #333;
    stroke-width: 1.5px;
    fill: none;
}
</style>


- - -

```bash
tmux
isub -n 2
module load hadoop-2.3.0
module load spark-0.9.0-hadoop-2.3.0

hadoop fs -mkdir /user
hadoop fs -mkdir /user/toniv 
hadoop fs -copyFromLocal data/NA12878.chrom19.SLX.maq.SRP000032.2009_07.coverage /user/toniv/
hadoop fs -copyFromLocal data/201101_encode_motifs_in_tf_peaks.bed /user/toniv/
hadoop fs -copyFromLocal data/Joyce-Ulysses.txt /user/toniv/
hadoop fs -copyFromLocal data/lpsa.data /user/toniv/
hdfs dfs -setrep -w 3 /user/toniv
# Start the Spark Shell
SPARK_MEM=32g MASTER=spark://ly-1-10:7077 bin/spark-shell
```

- - -

```bash
cd server
cat local.conf | sed '/^\ *master/ c\  master = "'"$MASTER"\"> local.tmp
cp local.conf local.backup
cp local.tmp local.conf
./server_start.sh
cd ~
```

- - -

```scala
val file = textFile("/var/log/system.log")
val words = file.flatMap(_.split(" "))
val mapped = words map (x => (x,1))
val grouped = mapped.groupBy(_._1)
val res = grouped map (x => x._2.length)
```




- - -

### Lineage

<div id="simpleLineage" align="center">
<svg width="600" height="500">
<g transform="translate(20,20)"/>
</svg>
</div>

<script>
Reveal.addEventListener( 'ready', function( event ) {
    // event.currentSlide, event.indexh, event.indexv
    // Create a new directed graph
    var g = new dagreD3.Digraph();

    g.addNode("a1", { label: "file" });
    g.addNode("a2", { label: "words" });
    g.addNode("a3", { label: "mapped" });
    g.addNode("a4", { label: "grouped" });
    g.addNode("a5", { label: "result" });

    g.addEdge(null, "a1", "a2", { label: "flatMap" });
    g.addEdge(null, "a2", "a3", { label: "map / sort" });
    g.addEdge(null, "a3", "a4", { label: "groupByKey" });
    g.addEdge(null, "a4", "a5", { label: "reducyByKey" });
    
    var renderer = new dagreD3.Renderer();
    renderer.edgeInterpolate('linear');
    var svgElement = d3.selectAll("#simpleLineage svg g");
    var layout = dagreD3.layout()
//                        .nodeSep(20)
//                        .rankDir("LR");
    renderer.layout(layout).run(g, svgElement);
} );
</script>






