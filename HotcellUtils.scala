package cse511

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object HotcellUtils {
  val coordinateStep = 0.01

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int =
  {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var result = 0
    coordinateOffset match
    {
      case 0 => result = Math.floor((inputString.split(",")(0).replace("(","").toDouble/coordinateStep)).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")","").toDouble/coordinateStep).toInt
      // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    return result
  }

  def timestampParser (timestampString: String): Timestamp =
  {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  def dayOfYear (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }

  // YOU NEED TO CHANGE THIS PART

  // Function to calculate the Getis-Ord G-score for each cell
  def calculateGsordScore(totalPickupPoint: Int, totalNeighbors: Int, numCells: Int, mean: Double, S: Double): Double =
  {
    var numerator = totalPickupPoint.toDouble - (mean * totalNeighbors.toDouble)
    var denominator = S * math.sqrt(((numCells.toDouble * totalNeighbors.toDouble) - math.pow(totalNeighbors.toDouble, 2)) / (numCells.toDouble - 1.0))
    var result = (numerator/denominator).toDouble
    return result
  }

  // Function to calculate the number of neighboring cells for a given cell in 3D space
  def calculateNeighbors(inputX: Int, inputY: Int, inputZ: Int, minX: Int, minY: Int, minZ: Int, maxX: Int, maxY: Int, maxZ: Int): Int =
  {
    // If input matches any min/max values, increment counter to get the number of missing neighbors.
    var neighbour_check = 0
    var total_neighbours = 26
    var missing_neighbours = 0

    // Check if the current cell is at the boundary
    if (inputX == minX || inputX == maxX) {
      neighbour_check += 1
    }
    if (inputY == minY || inputY == maxY) {
      neighbour_check += 1
    }
    if (inputZ == minZ || inputZ == maxZ) {
      neighbour_check += 1
    }

    // Determine the number of missing neighbors based on how many boundaries the cell is on
    missing_neighbours = neighbour_check match {
      case 0 => 0
      case 1 => 9
      case 2 => 15
      case 3 => 19
    }

    // Return the number of valid neighbors (total - missing)
    return total_neighbours - missing_neighbours
  }
  
}