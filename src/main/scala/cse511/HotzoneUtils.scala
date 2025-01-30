package cse511

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String): Boolean = {
    // Split rectangle and point strings to get coordinates
    val rectCoords = queryRectangle.split(",")
    val pointCoords = pointString.split(",")
    
    // Rectangle coordinates
    val rectX1 = rectCoords(0).trim.toDouble
    val rectY1 = rectCoords(1).trim.toDouble
    val rectX2 = rectCoords(2).trim.toDouble
    val rectY2 = rectCoords(3).trim.toDouble

    // Point coordinates
    val pointX = pointCoords(0).trim.toDouble
    val pointY = pointCoords(1).trim.toDouble

    // Calculate min and max bounds of the rectangle
    val minX = math.min(rectX1, rectX2)
    val maxX = math.max(rectX1, rectX2)
    val minY = math.min(rectY1, rectY2)
    val maxY = math.max(rectY1, rectY2)

    // Check if point is inside the rectangle
    return pointX >= minX && pointX <= maxX && pointY >= minY && pointY <= maxY
  }
}
