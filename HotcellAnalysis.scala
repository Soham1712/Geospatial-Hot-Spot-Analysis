package cse511

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
  {
    // Load the original data from a data source
    var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath)
    pickupInfo.createOrReplaceTempView("nyctaxitrips")
    pickupInfo.show()

    // Assign cell coordinates based on pickup points
    spark.udf.register("CalculateX", (pickupPoint: String) => ((HotcellUtils.CalculateCoordinate(pickupPoint, 0))))
    spark.udf.register("CalculateY", (pickupPoint: String) => ((HotcellUtils.CalculateCoordinate(pickupPoint, 1))))
    spark.udf.register("CalculateZ", (pickupTime: String) => ((HotcellUtils.CalculateCoordinate(pickupTime, 2))))
    
    pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
    val newCoordinateName = Seq("x", "y", "z")
    pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
    pickupInfo.show()

    pickupInfo.createOrReplaceTempView("pickupInfo")

    // Define the min and max of x, y, z
    val minX = -74.50/HotcellUtils.coordinateStep
    val maxX = -73.70/HotcellUtils.coordinateStep
    val minY = 40.50/HotcellUtils.coordinateStep
    val maxY = 40.90/HotcellUtils.coordinateStep
    val minZ = 1
    val maxZ = 31
    val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

    // Aggregate data to count the number of points in each cell
    val cellPointCounts = pickupInfo.groupBy("x", "y", "z").agg(count("*").alias("pointCount"))
    cellPointCounts.createOrReplaceTempView("cellPointCounts")

    // Calculate the sum of all point counts and the mean
    val totalPointCount = cellPointCounts.agg(sum("pointCount")).first().getLong(0)
    val mean = totalPointCount.toDouble / numCells

    // Calculate the standard deviation using Spark SQL functions
    val stdDevDF = cellPointCounts.withColumn("variance", pow(col("pointCount") - mean, 2))
    val sumVariance = stdDevDF.agg(sum("variance")).first().getDouble(0)
    val stdDev = math.sqrt(sumVariance / numCells)


    // Calculate neighbors' point count for each cell
    val neighborCounts = spark.sql(s"""
      SELECT c1.x, c1.y, c1.z,
             SUM(c2.pointCount) AS totalNeighbors, 
             COUNT(*) AS numNeighbors
      FROM cellPointCounts c1, cellPointCounts c2
      WHERE
        (c2.x >= c1.x - 1 AND c2.x <= c1.x + 1) AND
        (c2.y >= c1.y - 1 AND c2.y <= c1.y + 1) AND
        (c2.z >= c1.z - 1 AND c2.z <= c1.z + 1)
      GROUP BY c1.x, c1.y, c1.z
    """)


    // Join neighbor counts with original data
    val cellWithNeighbors = cellPointCounts.join(neighborCounts, Seq("x", "y", "z"))

    // UDF to calculate G-score
    val gScoreUDF = udf((pointCount: Int, totalNeighbors: Int, numNeighbors: Int) => {
      // Check for zero variance or no neighbors and return NaN if invalid
      if (stdDev == 0 || numNeighbors == 0) {
        Double.NaN
      } else {
        HotcellUtils.calculateGsordScore(totalNeighbors, numNeighbors, numCells.toInt, mean, stdDev)
      }
    })

    // Apply the G-score UDF
    val gScoreDF = cellWithNeighbors.withColumn("gScore", gScoreUDF(col("pointCount"), col("totalNeighbors"), col("numNeighbors")))

    // Sort by G-score in descending order and return top 50 cells with G-score
    val resultDF = gScoreDF.orderBy(desc("gScore")).limit(50).select("x", "y", "z", "gScore")
    resultDF.show()

    return resultDF
  }
}
