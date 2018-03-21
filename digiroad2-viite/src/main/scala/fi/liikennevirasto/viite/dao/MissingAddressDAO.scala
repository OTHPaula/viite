package fi.liikennevirasto.viite.dao

import fi.liikennevirasto.digiroad2.Point
import fi.liikennevirasto.digiroad2.asset.BoundingRectangle
import fi.liikennevirasto.digiroad2.oracle.{MassQuery, OracleDatabase}
import fi.liikennevirasto.viite.RoadType
import fi.liikennevirasto.viite.model.Anomaly
import slick.driver.JdbcDriver.backend.Database.dynamicSession
import slick.jdbc.StaticQuery.interpolation
import slick.jdbc.{GetResult, PositionedResult, StaticQuery => Q}

case class MissingRoadAddress(linkId: Long, startAddrMValue: Option[Long], endAddrMValue: Option[Long],
                              roadType: RoadType, roadNumber: Option[Long], roadPartNumber: Option[Long],
                              startMValue: Option[Double], endMValue: Option[Double], anomaly: Anomaly,geom: Seq[Point])

object MissingAddressDAO {

  private val defaultMissingQuery = s"""
        SELECT LINK_ID, START_ADDR_M, END_ADDR_M, ROAD_NUMBER, ROAD_PART_NUMBER, ANOMALY_CODE, START_M, END_M,
        (SELECT X FROM TABLE(SDO_UTIL.GETVERTICES(geometry)) t WHERE ID = 1) as X,
        (SELECT Y FROM TABLE(SDO_UTIL.GETVERTICES(geometry)) t WHERE ID = 1) as Y,
        (SELECT X FROM TABLE(SDO_UTIL.GETVERTICES(geometry)) t WHERE ID = 2) as X2,
        (SELECT Y FROM TABLE(SDO_UTIL.GETVERTICES(geometry)) t WHERE ID = 2) as Y2
        FROM MISSING_ROAD_ADDRESS
      """

  implicit val getMissingAddress= new GetResult[MissingRoadAddress]{
    def apply(r: PositionedResult) = {

      val linkId = r.nextLong()
      val startAddrMValue = r.nextLongOption()
      val endAddrMValue = r.nextLongOption()
      val roadType = RoadType.apply(r.nextInt())
      val roadNumber = r.nextLongOption()
      val roadPartNumber = r.nextLongOption()
      val anomaly = Anomaly.apply(r.nextInt())
      val startMValue = r.nextDoubleOption()
      val endMValue = r.nextDoubleOption()
      val x = r.nextDouble()
      val y = r.nextDouble()
      val x2 = r.nextDouble()
      val y2 = r.nextDouble()

      MissingRoadAddress(linkId, startAddrMValue, endAddrMValue, roadType, roadNumber, roadPartNumber,
        startMValue, endMValue, anomaly, Seq(Point(x,y), Point(x2,y2)))
    }
  }

  private def queryList(query: String): List[MissingRoadAddress] = {
    val tuples = Q.queryNA[MissingRoadAddress](query).list
    tuples.groupBy(_.linkId).map {
      case (_, missingAddressList) =>
        missingAddressList.head
    }.toList
  }

  private def massMissingAddress(query: String): String = {
    query + " JOIN TEMP_ID I ON I.ID = POS.LINK_ID"
  }

  private def getMissingAddresses(queryFilter: String => String, massQuery: Boolean = false): List[MissingRoadAddress] = {
    if (massQuery) {
      queryList(queryFilter(massMissingAddress(defaultMissingQuery)))
    } else {
      queryList(queryFilter(defaultMissingQuery))
    }
  }

  /**
    *
    *   FILTERS
    *
    */

  private def boundingBoxFilter(boundingRectangle: BoundingRectangle): String = {
    val extendedBoundingRectangle = BoundingRectangle(boundingRectangle.leftBottom + boundingRectangle.diagonal.scale(.15),
      boundingRectangle.rightTop - boundingRectangle.diagonal.scale(.15))
    OracleDatabase.boundingBoxFilter(extendedBoundingRectangle, "geometry")
  }

  private def withLinkIds(linkIds: Set[Long]): String = {
    s"link_id in (${linkIds.mkString(",")})"
  }

  /**
    *
    *   QUERY BUILDER
    *
    */

  private def byBoundingBox(boundingRectangle: BoundingRectangle)(query: String): String = {
    query + " WHERE " + boundingBoxFilter(boundingRectangle)
  }


  private def byLinkIds(ids: Set[Long])(query: String): String = {
    query + " WHERE " + withLinkIds(ids)
  }


  /** *
    *
    *   PUBLIC METHODS
    *
    */

  def createMissingRoadAddress (mra: MissingRoadAddress) = {
    val (p1, p2) = (mra.geom.head, mra.geom.last)

    sqlu"""
           INSERT INTO MISSING_ROAD_ADDRESS
           (SELECT ${mra.linkId}, ${mra.startAddrMValue}, ${mra.endAddrMValue},
             ${mra.roadNumber}, ${mra.roadPartNumber}, ${mra.anomaly.value},
             ${mra.startMValue}, ${mra.endMValue},
             MDSYS.SDO_GEOMETRY(4002, 3067, NULL, MDSYS.SDO_ELEM_INFO_ARRAY(1,2,1),
             MDSYS.SDO_ORDINATE_ARRAY(${p1.x},${p1.y},0.0,0.0,${p2.x},${p2.y},0.0,0.0))
              FROM dual WHERE NOT EXISTS (SELECT * FROM MISSING_ROAD_ADDRESS WHERE link_id = ${mra.linkId}) AND
              NOT EXISTS (SELECT * FROM ROAD_ADDRESS ra JOIN LRM_POSITION pos ON (pos.id = lrm_position_id)
                WHERE link_id = ${mra.linkId} AND (VALID_TO IS NULL OR VALID_TO > SYSDATE) ))
           """.execute
  }

  def createMissingRoadAddress (linkId: Long, start_addr_m: Long, end_addr_m: Long, anomaly_code: Int) = {
    sqlu"""
           insert into missing_road_address (link_id, start_addr_m, end_addr_m,anomaly_code)
           values ($linkId, $start_addr_m, $end_addr_m, $anomaly_code)
           """.execute
  }

  def createMissingRoadAddress (linkId: Long, start_addr_m: Long, end_addr_m: Long, anomaly_code: Int, start_m : Double, end_m : Double) = {
    sqlu"""
           insert into missing_road_address (link_id, start_addr_m, end_addr_m,anomaly_code, start_m, end_m)
           values ($linkId, $start_addr_m, $end_addr_m, $anomaly_code, $start_m, $end_m)
           """.execute
  }


  def fetchMissingRoadAddressesByBoundingBox(boundingRectangle: BoundingRectangle): Seq[MissingRoadAddress] = {
    getMissingAddresses(byBoundingBox(boundingRectangle))
  }

  def fetchByIdMassQuery(ids: Set[Long]): List[MissingRoadAddress] = {
    def getQuery()(query:String): String = {
      defaultMissingQuery
    }

    MassQuery.withIds(ids) {
      _ =>
        getMissingAddresses(getQuery(), massQuery = true)
    }
  }

  def getMissingRoadAddresses(linkIds: Set[Long]): List[MissingRoadAddress] = {
    if (linkIds.size > 500) {
      fetchByIdMassQuery(linkIds)
    } else {
      getMissingAddresses(byLinkIds(linkIds))
    }
  }

}
