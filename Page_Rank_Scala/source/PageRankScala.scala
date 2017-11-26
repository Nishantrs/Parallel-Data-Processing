package mapreducescala.pagerank

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import mapreducejava.fileparser.Bz2FileParser
import scala.collection.immutable.List
import org.apache.spark.HashPartitioner

object PageRankScala {

	def main(args: Array[String]) {

	  // Handling exceptions thrown by Spark application while running on EMR
		try{

			val conf = new SparkConf().
					setAppName("PageRank10Iteration").
					//For local run of spark application
					setMaster("local")
					//In order to run on EMR set the following as Master
					//setMaster("yarn")
					val sc = new SparkContext(conf)

					//Parsing initial list of unique nodes from the given .bz2 files
					var uniqueValidNodesFromInput = sc.textFile(args(0)) //Reading file from command line argument
					                                  .map(line => Bz2FileParser.format(line)) //Formatting raw text into a particular format
					                                  .filter(line => !line.contains("Invalid")) //Removing any invalid node containing '~' from list
					                                  .map(line => line.split("##")) //Splitting file based on delimiter used during format
					                                  .map(line => if(line.size == 1){(line(0),List())}else{(line(0), line(1).split("~").toList)})
					                                  // if outlinks from a node if zero assigning an empty list to it
					                                  .keyBy(line => line._1)// Creating a key value pair (nodeName,(nodeName,List[outlinksNames]))
					                                  .map(line => (line._1,line._2._2))// removing duplication of key into 
					                                                                    // format (nodeName,List[outlinksNames])

					uniqueValidNodesFromInput.persist() // Ensuring nodes around on the cluster for much faster access the 
					                                    // next time you query it

					// Extracting dangling nodes from the already computed unique list of nodes
					var updatedUniqueNodesWithDangling = uniqueValidNodesFromInput.values // list of outlinks for each nodes
					                                                              .flatMap { node => node } // Combining the whole list of outlinks
			                                                                  .keyBy(node => node) // Creating key-value pair for each outlinks in list
			                                                                  //.partitionBy(new HashPartitioner(10))
			                                                                  .reduceByKey((value1,value2) => value1.++(value2)) 
			                                                                  // Combining values multiple instance of key
			                                                                  .map(line => (line._1,List[String]())) // Creating key-value pair where
			                                                                                                         // key is outlinkName and 
			                                                                                                         // value is empty list

			// Computing new list of unique nodes comprising of outlinks 
			val finalUniqueNodesWithOutlinks = uniqueValidNodesFromInput.union(updatedUniqueNodesWithDangling)
			                                   // Returns an RDD containing data from both sources, duplicates are not removed
			                                                            //.partitionBy(new HashPartitioner(10))
			                                                            .reduceByKey((value1,value2) => value1.++(value2)) 
			                                                            // Combining values multiple instance of key
			
			// Total number of unique outlinks
			val noOfPages = finalUniqueNodesWithOutlinks.count()

			// For Testing Purpose
			//println ( "--------------------------------No Of Nodes-------------------------------- ") 
			//println(noOfPages)

			// Total number of iteration to be performed for page rank value calculation
			val ITERATION : Int = 10
			// Default page rank value for each node before starting of the iteration
			val INITIAL_PAGE_RANK: Double = (1.0 / noOfPages)
			// Constant for random probability jump
			val RANDOM_JUMP_PROBABILITY: Double = 0.15

			// Creating a new key-value pair which stores the page rank for each node
			var finalUniqueNodesWithPageRank = finalUniqueNodesWithOutlinks.keys // Considering just the nodeName 
			                                                               .map(line => (line,INITIAL_PAGE_RANK)) // assigning initial value

			
			// For Testing Purpose
			// println ( "--------------------------------No Of Nodes with PageRank-------------------------------- ") 
			// println(finalUniqueNodesWithPageRank.count())
			

			// Following code is referenced from :
			// https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/SparkPageRank.scala
			//
			// Iterating 10 times to calculate Page rank values  
			for (i <- 1 to ITERATION) { 

				try{

				  // Creating an accumulator to store the dangling factor
					var danglingFactor = sc.accumulator(0.0)
					
					    // Creating a key-value pair which stores all the details required for computing page rank for each node
					    // during each iteration
							var pageRankMatrix = finalUniqueNodesWithOutlinks.join(finalUniqueNodesWithPageRank)
							                     .values
							                     .flatMap {  
							                                case (outLinks, pageRank) => { 
							                                                                
								                              val size = outLinks.size  
										                           
								                              // If node is dangling
								                              if (size == 0) { 
								                              // Update dangling factor and assign empty list to it 
											                        danglingFactor += pageRank  
													                    List()  
										                          } else {  
										                          // Emit the contribution of page rank to all the outlinks
											                        outLinks.map(url => (url, pageRank / size))  
										                          }  
							                                                              }  
					                                  }
					// For Testing Purpose
					// println ( "--------------------------------No Of Nodes in Joined NodeList-------------------------------- ")
					// println(pageRankMatrix.count)
					// dangling statistics will be zero, if any action not performed on RDD
					pageRankMatrix.count

					// Storing the accumulated value to perform page rank calculation later
					val danglingValue : Double = danglingFactor.value 
					
					finalUniqueNodesWithPageRank = pageRankMatrix.reduceByKey(_ + _) // Combining the page rank contribution of other links to node
					// Computing page rank value
					.mapValues[Double](accumulatedPageRank => 
					  (RANDOM_JUMP_PROBABILITY*INITIAL_PAGE_RANK 
					      + (1 - RANDOM_JUMP_PROBABILITY)*(danglingValue / noOfPages + accumulatedPageRank)))
					
					// For Testing Purpose
					// println ( "--------------------------------No Of Nodes in Final PR NodeList-------------------------------- ")
					// println(finalUniqueNodesWithPageRank.count)

				} catch {
				case e:Exception => println("Exception caught: " + e);
				}
			}

			    // Following code is referenced from :
			    // http://stackoverflow.com/questions/26387753/how-to-reverse-ordering-for-rdd-takeordered                                            
			    var tempSorted = finalUniqueNodesWithPageRank.takeOrdered(100)(Ordering[Double]
			                                                 .reverse.on { line => line._2 });
			    
			    // Ensuring the sort list appear in one file
					var finalSorted = sc.parallelize(tempSorted).repartition(1)
					                                            .sortBy(line => line._2, false, 1)
					                                            .saveAsTextFile(args(1));
					
					
			    // val list = sc.parallelize(finalUniqueNodesWithPageRank.sortBy(line => line._2, false, 1).take(100))
					// list.repartition(1).saveAsTextFile(args(1))

		} catch {
		case e:Exception => println("Exception caught: " + e);
		}
	}

}