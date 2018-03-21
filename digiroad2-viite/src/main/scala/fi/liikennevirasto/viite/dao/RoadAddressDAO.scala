package fi.liikennevirasto.viite.dao

import java.sql.{PreparedStatement, Timestamp}
import java.text.DecimalFormat

import fi.liikennevirasto.digiroad2.{GeometryUtils, Point}
import fi.liikennevirasto.digiroad2.asset.{BoundingRectangle, LinkGeomSource, SideCode}
import fi.liikennevirasto.digiroad2.dao.Sequences
import fi.liikennevirasto.digiroad2.oracle.{MassQuery, OracleDatabase}
import fi.liikennevirasto.digiroad2.util.Track
import fi.liikennevirasto.viite.AddressConsistencyValidator.{AddressError, AddressErrorDetails}
import fi.liikennevirasto.viite.dao.CalibrationPointDAO.CalibrationPointMValues
import fi.liikennevirasto.viite.dao.TerminationCode.{NoTermination, Subsequent}
import fi.liikennevirasto.viite.model.RoadAddressLinkLike
import fi.liikennevirasto.viite.process.InvalidAddressDataException
import fi.liikennevirasto.viite.process.RoadAddressFiller.LRMValueAdjustment
import fi.liikennevirasto.viite.util.CalibrationPointsUtils
import fi.liikennevirasto.viite.{NewCommonHistoryId, NewRoadAddress, RoadCheckOptions, RoadType}
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import slick.driver.JdbcDriver.backend.Database.dynamicSession
import slick.jdbc.StaticQuery.interpolation
import slick.jdbc.{GetResult, PositionedResult, StaticQuery => Q}
import com.github.tototoshi.slick.MySQLJodaSupport._


sealed trait Discontinuity {
  def value: Int
  def description: String

  override def toString: String = s"$value - $description"
}
object Discontinuity {
  val values = Set(EndOfRoad, Discontinuous, ChangingELYCode, MinorDiscontinuity, Continuous)

  def apply(intValue: Int): Discontinuity = {
    values.find(_.value == intValue).getOrElse(Continuous)
  }

  def apply(longValue: Long): Discontinuity = {
    apply(longValue.toInt)
  }

  def apply(s: String): Discontinuity = {
    values.find(_.description.equalsIgnoreCase(s)).getOrElse(Continuous)
  }

  case object EndOfRoad extends Discontinuity { def value = 1; def description="Tien loppu"}
  case object Discontinuous extends Discontinuity { def value = 2 ; def description = "Epäjatkuva"}
  case object ChangingELYCode extends Discontinuity { def value = 3 ; def description = "ELY:n raja"}
  case object MinorDiscontinuity extends Discontinuity { def value = 4 ; def description= "Lievä epäjatkuvuus"}
  case object Continuous extends Discontinuity { def value = 5 ; def description = "Jatkuva"}
}

sealed trait CalibrationCode {
  def value: Int
}
object CalibrationCode {
  val values = Set(No, AtEnd, AtBeginning, AtBoth)

  def apply(intValue: Int): CalibrationCode = {
    values.find(_.value == intValue).getOrElse(No)
  }

  private def fromBooleans(beginning: Boolean, end: Boolean): CalibrationCode = {
    val beginValue = if (beginning) AtBeginning.value else No.value
    val endValue = if (end) AtEnd.value else No.value
    CalibrationCode.apply(beginValue+endValue)
  }
  def getFromAddress(roadAddress: BaseRoadAddress): CalibrationCode = {
    fromBooleans(roadAddress.calibrationPoints._1.isDefined, roadAddress.calibrationPoints._2.isDefined)
  }

  def getFromAddressLinkLike(roadAddress: RoadAddressLinkLike): CalibrationCode = {
    fromBooleans(roadAddress.startCalibrationPoint.isDefined, roadAddress.endCalibrationPoint.isDefined)
  }

  case object No extends CalibrationCode { def value = 0 }
  case object AtEnd extends CalibrationCode { def value = 1 }
  case object AtBeginning extends CalibrationCode { def value = 2 }
  case object AtBoth extends CalibrationCode { def value = 3 }
}
case class CalibrationPoint(linkId: Long, segmentMValue: Double, addressMValue: Long) extends CalibrationPointMValues

sealed trait TerminationCode {
  def value: Int
}

object TerminationCode {
  val values = Set(NoTermination, Termination, Subsequent)

  def apply(intValue: Int): TerminationCode = {
    values.find(_.value == intValue).getOrElse(NoTermination)
  }

  case object NoTermination extends TerminationCode { def value = 0 }
  case object Termination extends TerminationCode { def value = 1 }
  case object Subsequent extends TerminationCode { def value = 2 }
}

trait BaseRoadAddress {
  def id: Long
  def roadNumber: Long
  def roadPartNumber: Long
  def track: Track
  def discontinuity: Discontinuity
  def roadType: RoadType
  def startAddrMValue: Long
  def endAddrMValue: Long
  def startDate: Option[DateTime]
  def endDate: Option[DateTime]
  def modifiedBy: Option[String]
  def lrmPositionId : Long
  def linkId: Long
  def startMValue: Double
  def endMValue: Double
  def sideCode: SideCode
  def calibrationPoints: (Option[CalibrationPoint], Option[CalibrationPoint])
  def floating: Boolean
  def geometry: Seq[Point]
  def ely: Long
  def linkGeomSource: LinkGeomSource
  def reversed: Boolean
  def commonHistoryId: Long
  def blackUnderline: Boolean

  def copyWithGeometry(newGeometry: Seq[Point]): BaseRoadAddress

}

// Note: Geometry on road address is not directed: it isn't guaranteed to have a direction of digitization or road addressing
case class RoadAddress(id: Long, roadNumber: Long, roadPartNumber: Long, roadType: RoadType, track: Track,
                       discontinuity: Discontinuity, startAddrMValue: Long, endAddrMValue: Long,
                       startDate: Option[DateTime] = None, endDate: Option[DateTime] = None, modifiedBy: Option[String] = None,
                       lrmPositionId: Long, linkId: Long, startMValue: Double, endMValue: Double, sideCode: SideCode,
                       adjustedTimestamp: Long, calibrationPoints: (Option[CalibrationPoint], Option[CalibrationPoint]) = (None, None),
                       floating: Boolean = false, geometry: Seq[Point], linkGeomSource: LinkGeomSource, ely: Long,
                       terminated: TerminationCode = NoTermination,commonHistoryId: Long, blackUnderline: Boolean = false) extends BaseRoadAddress {
  val endCalibrationPoint = calibrationPoints._2
  val startCalibrationPoint = calibrationPoints._1

  def reversed: Boolean = false

  def addressBetween(a: Double, b: Double): (Long, Long) = {
    val (addrA, addrB) = (addrAt(a), addrAt(b))
    (Math.min(addrA,addrB), Math.max(addrA,addrB))
  }

  private def addrAt(a: Double) = {
    val coefficient = (endAddrMValue - startAddrMValue) / (endMValue - startMValue)
    sideCode match {
      case SideCode.AgainstDigitizing =>
        endAddrMValue - Math.round((a-startMValue) * coefficient)
      case SideCode.TowardsDigitizing =>
        startAddrMValue + Math.round((a-startMValue) * coefficient)
      case _ => throw new InvalidAddressDataException(s"Bad sidecode $sideCode on road address $id (link $linkId)")
    }
  }

  def copyWithGeometry(newGeometry: Seq[Point]) = {
    this.copy(geometry = newGeometry)
  }
}


object RoadAddressDAO {

  val formatter = ISODateTimeFormat.dateOptionalTimeParser()

  def dateTimeParse(string: String) = {
    formatter.parseDateTime(string)
  }

  val dateFormatter = ISODateTimeFormat.basicDate()

  def optDateTimeParse(string: String): Option[DateTime] = {
    try {
      if (string==null || string == "")
        None
      else
        Some(DateTime.parse(string, formatter))
    } catch {
      case ex: Exception => None
    }
  }

  private val defaultQuery =
    s"""
        SELECT RA.ID, RA.ROAD_NUMBER, RA.ROAD_PART_NUMBER, RA.ROAD_TYPE, RA.TRACK_CODE,
        RA.DISCONTINUITY, RA.START_ADDR_M, RA.END_ADDR_M, RA.LRM_POSITION_ID, POS.LINK_ID, POS.START_MEASURE, POS.END_MEASURE,
        POS.SIDE_CODE, POS.ADJUSTED_TIMESTAMP,
        RA.START_DATE, RA.END_DATE, RA.CREATED_BY, RA.VALID_FROM, RA.CALIBRATION_POINTS, RA.FLOATING, T.X, T.Y, T2.X, T2.Y,
        LINK_SOURCE, RA.ELY, RA.TERMINATED, RA.COMMON_HISTORY_ID
        FROM ROAD_ADDRESS RA CROSS JOIN
        TABLE(SDO_UTIL.GETVERTICES(RA.GEOMETRY)) T CROSS JOIN
        TABLE(SDO_UTIL.GETVERTICES(RA.GEOMETRY)) T2
        JOIN LRM_POSITION POS ON RA.LRM_POSITION_ID = POS.ID"""

  private val queryWithLatestNetwork =
    s"""
        SELECT RA.ID, RA.ROAD_NUMBER, RA.ROAD_PART_NUMBER, RA.ROAD_TYPE, RA.TRACK_CODE,
        RA.DISCONTINUITY, RA.START_ADDR_M, RA.END_ADDR_M, RA.LRM_POSITION_ID, POS.LINK_ID, POS.START_MEASURE, POS.END_MEASURE,
        POS.SIDE_CODE, POS.ADJUSTED_TIMESTAMP,
        RA.START_DATE, RA.END_DATE, RA.CREATED_BY, RA.VALID_FROM, RA.CALIBRATION_POINTS, RA.FLOATING, T.X, T.Y, T2.X, T2.Y,
        LINK_SOURCE, RA.ELY, RA.TERMINATED, RA.COMMON_HISTORY_ID,
        NET.ID AS ROAD_VERSION, NET.CREATED AS VERSION_DATE
        FROM ROAD_ADDRESS RA CROSS JOIN
        TABLE(SDO_UTIL.GETVERTICES(RA.GEOMETRY)) T CROSS JOIN
        TABLE(SDO_UTIL.GETVERTICES(RA.GEOMETRY)) T2
        JOIN LRM_POSITION POS ON RA.LRM_POSITION_ID = POS.ID
        JOIN PUBLISHED_ROAD_NETWORK NET ON NET.ID = (SELECT MAX(NETWORK_ID) FROM PUBLISHED_ROAD_ADDRESS WHERE RA.ID = ROAD_ADDRESS_ID)"""

  implicit val getRoadAddress= new GetResult[RoadAddress]{
    def apply(r: PositionedResult) = {

      val id = r.nextLong()
      val roadNumber = r.nextLong()
      val roadPartNumber = r.nextLong()
      val roadType = RoadType.apply(r.nextInt())
      val trackCode = r.nextInt()
      val discontinuity = r.nextInt()
      val startAddrMValue = r.nextLong()
      val endAddrMValue = r.nextLong()
      val lrmPositionId = r.nextLong()
      val linkId = r.nextLong()
      val startMValue = r.nextDouble()
      val endMValue = r.nextDouble()
      val sideCode = r.nextInt()
      val adjustedTimestamp = r.nextLong()
      val startDate = r.nextDateOption.map(new DateTime(_))
      val endDate = r.nextDateOption.map(new DateTime(_))
      val createdBy = r.nextStringOption.map(new String(_))
      val createdDate = r.nextDateOption.map(new DateTime(_))
      val calibrationCode = r.nextInt()
      val floating = r.nextBoolean()
      val x = r.nextDouble()
      val y = r.nextDouble()
      val x2 = r.nextDouble()
      val y2 = r.nextDouble()
      val geomSource = LinkGeomSource.apply(r.nextInt)
      val ely = r.nextLong()
      val terminated = TerminationCode.apply(r.nextInt())
      val commonHistoryId = r.nextLong()
      RoadAddress(id, roadNumber, roadPartNumber, roadType, Track.apply(trackCode), Discontinuity.apply(discontinuity),
        startAddrMValue, endAddrMValue, startDate, endDate, createdBy, lrmPositionId, linkId, startMValue, endMValue,
        SideCode.apply(sideCode), adjustedTimestamp, CalibrationPointsUtils.calibrations(CalibrationCode.apply(calibrationCode),
          linkId, startMValue, endMValue, startAddrMValue, endAddrMValue, SideCode.apply(sideCode)), floating,
        Seq(Point(x,y), Point(x2,y2)), geomSource, ely, terminated, commonHistoryId)
    }
  }

  private def queryList(query: String): List[RoadAddress] = {
    val tuples = Q.queryNA[RoadAddress](query).list
    tuples.groupBy(_.id).map {
      case (_, roadAddressList) =>
        roadAddressList.head
    }.toList
  }

  private def defaultRoadAddress(query: String): String = {
    query + " WHERE T.ID < T2.ID"
  }

  private def massRoadAddress(query: String): String = {
    defaultRoadAddress(query + " JOIN TEMP_ID I ON I.ID = POS.LINK_ID")
  }

  private def getRoadAddresses(queryFilter: String => String, massQuery: Boolean = false, withLatestNetwork: Boolean = false): List[RoadAddress] = {
    val roadAddressQuery = if (withLatestNetwork) queryWithLatestNetwork else defaultQuery
    if (massQuery) {
      queryList(queryFilter(massRoadAddress(roadAddressQuery)))
    } else {
      queryList(queryFilter(defaultRoadAddress(roadAddressQuery)))
    }
  }

  private def toTimeStamp(dateTime: Option[DateTime]) = {
    dateTime.map(dt => new Timestamp(dt.getMillis))
  }

  /***
    *
    *   FILTERS
    *
    */

  private def withRoadNumber(roadNumber: Long): String = {
    s" AND RA.ROAD_NUMBER = $roadNumber"
  }

  private def withRoadPartNumber(roadPartNumber: Long): String = {
    s" AND RA.ROAD_PART_NUMBER = $roadPartNumber"
  }

  private def withEly(ely: Long): String = {
    s" AND RA.ELY = $ely"
  }

  /*private def withLinkId(linkId: Long): String = {
    s" AND POS.LINK_ID = $linkId"
  }*/

  private def withLinkIds(linkIds: Set[Long]): String = {
    if (linkIds.isEmpty) {
      ""
    } else {
      s"AND POS.LINK_ID IN (${linkIds.mkString(",")})"
    }
  }

  private def onlyFloating(floating: Boolean = false): String = {
    if (floating) s" AND RA.FLOATING = 1" else s""
  }

  private def withFloating(floating: Boolean = true): String = {
    if (!floating) s" AND RA.FLOATING = 0" else s""
  }

  private def withHistory(withHistory: Boolean = false): String = {
    if (!withHistory) s" AND RA.END_DATE IS NULL" else ""
  }

  private def withSuravage(withSuravage: Boolean = true): String = {
    if (!withSuravage) s" AND POS.LINK_SOURCE != 3" else s""
  }

  private def withTerminated(withTerminated: Boolean = true): String = {
    if (!withTerminated) s" AND RA.TERMINATED = 0" else s""
  }

  private def validRoadAddress(withValid: Boolean = true): String = {
    if (withValid) {
      s" AND RA.VALID_FROM <= SYSDATE AND (RA.VALID_TO IS NULL OR RA.VALID_TO > SYSDATE)"
    } else ""
  }

  private def onlyNormalRoads(onlyNormal: Boolean): String = {
    if (onlyNormal) {
      s" AND POS.LINK_SOURCE =1"
    } else ""
  }

  private def withIdFilter(filterIds: Set[Long], in: Boolean): String = {
    val inFilter = if (in) s"IN" else s"NOT IN"
    if (filterIds.nonEmpty) {
      s"AND RA.ID $inFilter ${filterIds.mkString("(", ",", ")")}"
    } else ""
  }

  private def boundingBoxFilter(boundingRectangle: BoundingRectangle): String = {
    val extendedBoundingRectangle = BoundingRectangle(boundingRectangle.leftBottom + boundingRectangle.diagonal.scale(.15),
      boundingRectangle.rightTop - boundingRectangle.diagonal.scale(.15))
    OracleDatabase.boundingBoxFilter(extendedBoundingRectangle, "geometry")
  }

  protected def withRoadNumbersFilter(roadNumbers: Seq[(Int, Int)], filter: String = ""): String = {
    if (roadNumbers.isEmpty)
      return s" AND ($filter)"
    val limit = roadNumbers.head
    val filterAdd = s"""(RA.ROAD_NUMBER >= ${limit._1} AND RA.ROAD_NUMBER <= ${limit._2})"""
    if (filter == "")
      withRoadNumbersFilter(roadNumbers.tail,  filterAdd)
    else
      withRoadNumbersFilter(roadNumbers.tail,  s"""$filter OR $filterAdd""")
  }

  /***
    *
    *   QUERY BUILDERS
    *
    */

  private def byBoundingBox(boundingRectangle: BoundingRectangle, fetchOnlyFloating: Boolean, normalRoads: Boolean, roadNumberLimits: Seq[(Int, Int)])(query: String): String = {
    query + validRoadAddress() + boundingBoxFilter(boundingRectangle) + onlyFloating(fetchOnlyFloating) + onlyNormalRoads(normalRoads) + withRoadNumbersFilter(roadNumberLimits)
  }

  private def byRoadPart(roadNumber: Long, roadPartNumber: Long, includeFloating: Boolean = false, includeExpired: Boolean = false,
                         includeHistory: Boolean, includeSuravage: Boolean)(query: String): String = {
    query + withRoadNumber(roadNumber) + withRoadPartNumber(roadPartNumber) + withFloating(includeFloating) + validRoadAddress(includeExpired) +
      withHistory(includeHistory) + withSuravage(includeSuravage)
  }

  private def byRoad(roadNumber: Long, includeFloating: Boolean)(query: String): String = {
    query + withRoadNumber(roadNumber) + withFloating(includeFloating)
  }

  private def partsByRoadNumbers(boundingRectangle: BoundingRectangle, roadNumbers: Seq[(Int, Int)], coarse: Boolean)(query: String): String = {
    val roadsFilter = roadNumbers.map(n => withRoadNumber(n._1) + withRoadPartNumber(n._2)).mkString("(", ") OR (", ")")
    val trackCode = if (roadNumbers.nonEmpty) s" AND RA.TRACK_CODE IN (0,1)"
    val calibration = if (coarse) " AND RA.CALIBRATION_POINTS != 0"
    query + validRoadAddress() + boundingBoxFilter(boundingRectangle) + trackCode + roadsFilter + calibration
  }

  private def byLinkId(linkIds: Set[Long], includeFloating: Boolean, includeHistory: Boolean, includeTerminated: Boolean,
                       filterIds: Set[Long])(query: String): String = {
    query + validRoadAddress() + withFloating(includeFloating) + withHistory(includeHistory) + withTerminated(includeTerminated) + withIdFilter(filterIds, in = false)
  }

  private def byId(ids: Set[Long], includeHistory: Boolean, includeTerminated: Boolean)(query: String): String = {
    query + validRoadAddress() + withIdFilter(ids, in = true) + withTerminated(includeTerminated) + withTerminated(includeTerminated)
  }

  private def byIdMassQuery(includeFloating: Boolean, includeHistory: Boolean)(query: String): String = {
    query + validRoadAddress() + withFloating(includeFloating) + withHistory(includeHistory)
  }

  private def allFloating(includesHistory: Boolean)(query: String) : String = {
    query + validRoadAddress() + withHistory(includesHistory)
  }

  private def byAddress(roadNumber: Long, roadPartNumber: Long, track: Track, startAddrM: Option[Long], endAddrM: Option[Long])(query: String): String = {
    val startFilter = startAddrM.map(s => s" AND start_addr_m = $s").getOrElse("")
    val endFilter = endAddrM.map(e => s" AND end_addr_m = $e").getOrElse("")
    val trackFilter = s" AND RA.TRACK_CODE = ${track.value}"
    query + validRoadAddress() + withRoadNumber(roadNumber) + withRoadPartNumber(roadPartNumber) + trackFilter + startFilter + endFilter
  }

  private def fetchByAddress(roadNumber: Long, roadPartNumber: Long, track: Track, startAddrM: Option[Long], endAddrM: Option[Long])= {
    getRoadAddresses(byAddress(roadNumber, roadPartNumber, track, startAddrM, endAddrM)).headOption
  }

  private def byLinkIdToApi(linkIds: Set[Long], ignoreHistory: Boolean)(query: String): String = {
    query + validRoadAddress() + withLinkIds(linkIds) + withHistory(ignoreHistory)
  }

  private def allCurrentRoads(options: RoadCheckOptions)(query: String): String = {
    val road = if (options.roadNumbers.nonEmpty) {
      s"AND ROAD_NUMBER in (${options.roadNumbers.mkString(",")})"
    } else ""
    query + validRoadAddress() + withTerminated(false) + withFloating(false)  + road
  }

  private def byEly(ely: Long)(query: String): String = {
    query + withEly(ely)
  }


  /** *
    *
    *   PUBLIC METHODS
    *
    */

  def create(roadAddresses: Iterable[RoadAddress], createdBy : Option[String] = None): Seq[Long] = {

    val lrmPositionPS = dynamicSession.prepareStatement("insert into lrm_position (ID, link_id, SIDE_CODE, start_measure, end_measure, adjusted_timestamp, link_source) values (?, ?, ?, ?, ?, ?, ?)")
    val addressPS = dynamicSession.prepareStatement("insert into ROAD_ADDRESS (id, lrm_position_id, road_number, road_part_number, " +
      "track_code, discontinuity, START_ADDR_M, END_ADDR_M, start_date, end_date, created_by, " +
      "VALID_FROM, geometry, floating, calibration_points, ely, road_type, terminated, common_history_id) values (?, ?, ?, ?, ?, ?, ?, ?, TO_DATE(?, 'YYYY-MM-DD'), " +
      "TO_DATE(?, 'YYYY-MM-DD'), ?, sysdate, MDSYS.SDO_GEOMETRY(4002, 3067, NULL, MDSYS.SDO_ELEM_INFO_ARRAY(1,2,1), MDSYS.SDO_ORDINATE_ARRAY(" +
      "?,?,0.0,?,?,?,0.0,?)), ?, ?, ? ,?, ?, ?)")
    val ids = sql"""SELECT lrm_position_primary_key_seq.nextval FROM dual connect by level <= ${roadAddresses.size}""".as[Long].list
    val (ready, idLess) = roadAddresses.partition(_.id != NewRoadAddress)
    val plIds = Sequences.fetchViitePrimaryKeySeqValues(idLess.size)
    val createAddresses = ready ++ idLess.zip(plIds).map(x =>
      x._1.copy(id = x._2)
    )
    val savedIds = createAddresses.zip(ids).foreach { case ((address), (lrmId)) =>
      createLRMPosition(lrmPositionPS, lrmId, address.linkId, address.sideCode.value, address.startMValue,
        address.endMValue, address.adjustedTimestamp, address.linkGeomSource.value)
      val nextId = if (address.id == NewRoadAddress)
        Sequences.nextViitePrimaryKeySeqValue
      else address.id
      val nextCommonHistoryId = if (address.commonHistoryId == NewCommonHistoryId)
        Sequences.nextCommonHistorySeqValue
      else address.commonHistoryId
      addressPS.setLong(1, nextId)
      addressPS.setLong(2, lrmId)
      addressPS.setLong(3, address.roadNumber)
      addressPS.setLong(4, address.roadPartNumber)
      addressPS.setLong(5, address.track.value)
      addressPS.setLong(6, address.discontinuity.value)
      addressPS.setLong(7, address.startAddrMValue)
      addressPS.setLong(8, address.endAddrMValue)
      addressPS.setString(9, address.startDate match {
        case Some(dt) => dateFormatter.print(dt)
        case None => ""
      })
      addressPS.setString(10, address.endDate match {
        case Some(dt) => dateFormatter.print(dt)
        case None => ""
      })
      val newCreatedBy = createdBy.getOrElse(address.modifiedBy.getOrElse("-"))
      addressPS.setString(11, if(newCreatedBy == null) "-" else newCreatedBy)
      val (p1, p2) = (address.geometry.head, address.geometry.last)
      addressPS.setDouble(12, p1.x)
      addressPS.setDouble(13, p1.y)
      addressPS.setDouble(14, address.startAddrMValue)
      addressPS.setDouble(15, p2.x)
      addressPS.setDouble(16, p2.y)
      addressPS.setDouble(17, address.endAddrMValue)
      addressPS.setInt(18, if (address.floating) 1 else 0)
      addressPS.setInt(19, CalibrationCode.getFromAddress(address).value)
      addressPS.setLong(20, address.ely)
      addressPS.setInt(21, address.roadType.value)
      addressPS.setInt(22, address.terminated.value)
      addressPS.setLong(23, nextCommonHistoryId)
      addressPS.addBatch()
    }
    lrmPositionPS.executeBatch()
    addressPS.executeBatch()
    lrmPositionPS.close()
    addressPS.close()
    createAddresses.map(_.id).toSeq
  }

  def createLRMPosition(lrmPositionPS: PreparedStatement, id: Long, linkId: Long, sideCode: Int,
                        startM: Double, endM: Double, adjustedTimestamp : Long, geomSource: Int): Unit = {
    lrmPositionPS.setLong(1, id)
    lrmPositionPS.setLong(2, linkId)
    lrmPositionPS.setLong(3, sideCode)
    lrmPositionPS.setDouble(4, startM)
    lrmPositionPS.setDouble(5, endM)
    lrmPositionPS.setDouble(6, adjustedTimestamp)
    lrmPositionPS.setInt(7, geomSource)
    lrmPositionPS.addBatch()
  }

  def update(roadAddress: RoadAddress, geometry: Option[Seq[Point]]) : Unit = {
    if (geometry.isEmpty)
      updateWithoutGeometry(roadAddress)
    else {
      val startTS = toTimeStamp(roadAddress.startDate)
      val endTS = toTimeStamp(roadAddress.endDate)
      val first = geometry.get.head
      val last = geometry.get.last
      val (x1, y1, z1, x2, y2, z2) = (first.x, first.y, first.z, last.x, last.y, last.z)
      val length = GeometryUtils.geometryLength(geometry.get)
      sqlu"""UPDATE ROAD_ADDRESS
        SET road_number = ${roadAddress.roadNumber},
           road_part_number= ${roadAddress.roadPartNumber},
           track_code = ${roadAddress.track.value},
           discontinuity= ${roadAddress.discontinuity.value},
           START_ADDR_M= ${roadAddress.startAddrMValue},
           END_ADDR_M= ${roadAddress.endAddrMValue},
           start_date= $startTS,
           end_date= $endTS,
           geometry= MDSYS.SDO_GEOMETRY(4002, 3067, NULL, MDSYS.SDO_ELEM_INFO_ARRAY(1,2,1), MDSYS.SDO_ORDINATE_ARRAY(
             $x1,$y1,$z1,0.0,$x2,$y2,$z2,$length))
        WHERE id = ${roadAddress.id}""".execute
    }
  }

  def updateGeometry(roadAddressId: Long, geometry: Seq[Point]): Unit = {
    if(!geometry.isEmpty){
      val newFormat = new DecimalFormat("#.###");
      val first = geometry.head

      val last = geometry.last
      val (x1, y1, z1, x2, y2, z2) = (newFormat.format(first.x), newFormat.format(first.y), newFormat.format(first.z), newFormat.format(last.x), newFormat.format(last.y), newFormat.format(last.z))
      val length = GeometryUtils.geometryLength(geometry)
      sqlu"""UPDATE ROAD_ADDRESS
        SET geometry= MDSYS.SDO_GEOMETRY(4002, 3067, NULL, MDSYS.SDO_ELEM_INFO_ARRAY(1,2,1), MDSYS.SDO_ORDINATE_ARRAY(
             $x1,$y1,$z1,0.0,$x2,$y2,$z2,$length))
        WHERE id = ${roadAddressId}""".execute
    }
  }

  private def updateWithoutGeometry(roadAddress: RoadAddress) = {
    val startTS = toTimeStamp(roadAddress.startDate)
    val endTS = toTimeStamp(roadAddress.endDate)
    sqlu"""UPDATE ROAD_ADDRESS
        SET road_number = ${roadAddress.roadNumber},
           road_part_number= ${roadAddress.roadPartNumber},
           track_code = ${roadAddress.track.value},
           discontinuity= ${roadAddress.discontinuity.value},
           START_ADDR_M= ${roadAddress.startAddrMValue},
           END_ADDR_M= ${roadAddress.endAddrMValue},
           start_date= $startTS,
           end_date= $endTS
        WHERE id = ${roadAddress.id}""".execute
  }

  def expireById(ids: Set[Long]): Int = {
    val query =
      s"""
          UPDATE ROAD_ADDRESS SET VALID_TO = SYSDATE WHERE VALID_TO IS NULL AND ID IN (${ids.mkString(",")})
        """
    if (ids.isEmpty)
      0
    else
      Q.updateNA(query).first
  }

  def changeRoadAddressFloating(isFloating: Boolean, roadAddressId: Long, geometry: Option[Seq[Point]] = None): Unit = {
    val float = if (isFloating) 1 else 0
    if (geometry.nonEmpty) {
      val first = geometry.get.head
      val last = geometry.get.last
      val (x1, y1, z1, x2, y2, z2) = (first.x, first.y, first.z, last.x, last.y, last.z)
      val length = GeometryUtils.geometryLength(geometry.get)
      sqlu"""
           UPDATE ROAD_ADDRESS SET FLOATING = $float,
                  GEOMETRY = MDSYS.SDO_GEOMETRY(4002, 3067, NULL, MDSYS.SDO_ELEM_INFO_ARRAY(1,2,1), MDSYS.SDO_ORDINATE_ARRAY(
                  $x1,$y1,$z1,0.0,$x2,$y2,$z2,$length))
             WHERE ID = $roadAddressId
      """.execute
    } else {
      sqlu"""
           UPDATE ROAD_ADDRESS SET FLOATING = $float
             WHERE ID = $roadAddressId
      """.execute
    }
  }

  def updateLRM(lRMValueAdjustment: LRMValueAdjustment) = {
    val (startM, endM) = (lRMValueAdjustment.startMeasure, lRMValueAdjustment.endMeasure)
    (startM, endM) match {
      case (Some(s), Some(e)) =>
        sqlu"""
           UPDATE LRM_POSITION
           SET start_measure = $s,
             end_measure = $e,
             link_id = ${lRMValueAdjustment.linkId},
             modified_date = sysdate
           WHERE id = (SELECT LRM_POSITION_ID FROM ROAD_ADDRESS WHERE id = ${lRMValueAdjustment.addressId})
      """.execute
      case (_, Some(e)) =>
        sqlu"""
           UPDATE LRM_POSITION
           SET
             end_measure = ${lRMValueAdjustment.endMeasure.get},
             link_id = ${lRMValueAdjustment.linkId},
             modified_date = sysdate
           WHERE id = (SELECT LRM_POSITION_ID FROM ROAD_ADDRESS WHERE id = ${lRMValueAdjustment.addressId})
      """.execute
      case (Some(s), _) =>
        sqlu"""
           UPDATE LRM_POSITION
           SET start_measure = ${lRMValueAdjustment.startMeasure.get},
             link_id = ${lRMValueAdjustment.linkId},
             modified_date = sysdate
           WHERE id = (SELECT LRM_POSITION_ID FROM ROAD_ADDRESS WHERE id = ${lRMValueAdjustment.addressId})
      """.execute
      case _ =>
    }
  }

  def updateLRM(id: Long, geometrySource: LinkGeomSource): Boolean = {
    sqlu"""
           UPDATE LRM_POSITION SET link_source = ${geometrySource.value} WHERE id = $id
      """.execute
    true
  }

  def roadPartExists(roadNumber:Long, roadPart:Long) :Boolean = {
    val query = s"""SELECT COUNT(1)
            FROM road_address
             WHERE road_number=$roadNumber AND road_part_number=$roadPart AND ROWNUM < 2"""
    if (Q.queryNA[Int](query).first>0) true else false
  }

  def roadNumberExists(roadNumber:Long) :Boolean = {
    val query = s"""SELECT COUNT(1)
            FROM road_address
             WHERE road_number=$roadNumber AND ROWNUM < 2"""
    if (Q.queryNA[Int](query).first>0) true else false
  }

  def getCurrentValidRoadNumbers(filter: String = "") = {
    Q.queryNA[Long](s"""
       select distinct road_number
              from road_address ra
              where ra.floating = '0' AND (end_date > sysdate OR end_date IS NULL) AND (valid_to > sysdate OR valid_to IS NULL)
              $filter
              order by road_number
      """).list
  }

  def getValidRoadNumbersWithFilterToTestAndDevEnv = {
    getCurrentValidRoadNumbers("AND (ra.road_number <= 20000 OR (ra.road_number >= 40000 AND ra.road_number <= 70000) OR ra.road_number > 99999 )")
  }

  def getValidRoadParts(roadNumber: Long) = {
    sql"""
       select distinct road_part_number
              from road_address ra
              where road_number = $roadNumber AND (valid_to > sysdate OR valid_to IS NULL)
              AND (END_DATE IS NULL OR END_DATE > sysdate)
      """.as[Long].list
  }

  def getValidRoadParts(roadNumber: Long, startDate: DateTime) = {
    sql"""
       select distinct ra.road_part_number
              from road_address ra
              where road_number = $roadNumber AND (valid_to > sysdate OR valid_to IS NULL) AND START_DATE <= $startDate
              AND (END_DATE IS NULL OR END_DATE > $startDate)
              AND ra.road_part_number NOT IN (select distinct pl.road_part_number from project_link pl where (select count(distinct pl2.status) from project_link pl2 where pl2.road_part_number = ra.road_part_number and pl2.road_number = ra.road_number)
               = 1 and pl.status = 5)
      """.as[Long].list
  }

  def fetchNextRoadPartNumber(current: Int, roadNumber: Option[Int] = None) = {
    val road = if (roadNumber.nonEmpty) withRoadNumber(roadNumber.get) else ""
    val query =
      s"""
          SELECT * FROM (
            SELECT ra.road_part_number
            FROM road_address ra
            WHERE road_part_number > $current $road AND (valid_to > sysdate OR valid_to IS NULL)
            ORDER BY road_part_number ASC
          ) WHERE ROWNUM < 2
      """
    Q.queryNA[Int](query).firstOption
  }

  def fetchRoadAddressesByBoundingBox(boundingRectangle: BoundingRectangle, fetchOnlyFloating: Boolean, onlyNormalRoads: Boolean = false, roadNumberLimits: Seq[(Int, Int)] = Seq()): (Seq[RoadAddress]) = {
    getRoadAddresses(byBoundingBox(boundingRectangle, fetchOnlyFloating, onlyNormalRoads, roadNumberLimits))
  }

  def fetchByRoadPart(roadNumber: Long, roadPartNumber: Long, includeFloating: Boolean = false, includeExpired: Boolean = false,
                      includeHistory: Boolean = false, includeSuravage: Boolean = true): List[RoadAddress] = {

    getRoadAddresses(byRoadPart(roadNumber, roadPartNumber, includeFloating, includeExpired, includeHistory, includeSuravage))
  }

  def fetchPartsByRoadNumbers(boundingRectangle: BoundingRectangle, roadNumbers: Seq[(Int, Int)], coarse: Boolean = false): List[RoadAddress] = {
    getRoadAddresses(partsByRoadNumbers(boundingRectangle, roadNumbers, coarse))
  }

  def fetchByIdMassQuery(ids: Set[Long], includeFloating: Boolean = false, includeHistory: Boolean = true): List[RoadAddress] = {
    MassQuery.withIds(ids) {
      _ =>
        getRoadAddresses(byIdMassQuery(includeFloating, includeHistory), massQuery = true)
    }
  }

  def fetchByLinkId(linkIds: Set[Long], includeFloating: Boolean = false, includeHistory: Boolean = true, includeTerminated: Boolean = true,
                    filterIds: Set[Long] = Set()): List[RoadAddress] = {
    if (linkIds.size > 1000 || filterIds.size > 1000) {
      fetchByIdMassQuery(linkIds, includeFloating, includeHistory).filterNot(ra => filterIds.contains(ra.id))
    } else {
      getRoadAddresses(byLinkId(linkIds, includeFloating, includeHistory, includeTerminated, filterIds))
    }
  }

  def fetchAllFloatingRoadAddresses(includesHistory: Boolean = false) = {
    getRoadAddresses(allFloating(includesHistory))
  }

  def queryById(ids: Set[Long], includeHistory: Boolean = false, includeTerminated: Boolean = false): List[RoadAddress] = {
    if (ids.size > 1000) {
      fetchByIdMassQuery(ids)
    } else {
      getRoadAddresses(byId(ids, includeHistory, includeTerminated))
    }
  }

  def fetchByAddressStart(roadNumber: Long, roadPartNumber: Long, track: Track, startAddrM: Long): Option[RoadAddress] = {
    fetchByAddress(roadNumber, roadPartNumber, track, Some(startAddrM), None)
  }
  def fetchByAddressEnd(roadNumber: Long, roadPartNumber: Long, track: Track, endAddrM: Long): Option[RoadAddress] = {
    fetchByAddress(roadNumber, roadPartNumber, track, None, Some(endAddrM))
  }

  def getRoadPartInfo(roadNumber:Long, roadPart:Long): Option[(Long,Long,Long,Long,Long,Option[DateTime],Option[DateTime])] =
  {
    val query =
      s"""SELECT r.id, l.link_id, r.end_addr_M, r.discontinuity, r.ely, (Select Max(ra.start_date) from road_address ra
          Where r.ROAD_PART_NUMBER = ra.ROAD_PART_NUMBER and r.ROAD_NUMBER = ra.ROAD_NUMBER) as start_date,
           (Select Max(ra.end_Date) from road_address ra Where r.ROAD_PART_NUMBER = ra.ROAD_PART_NUMBER and r.ROAD_NUMBER = ra.ROAD_NUMBER)
           as end_date FROM road_address r INNER JOIN lrm_position l ON r.lrm_position_id =  l.id
           INNER JOIN (Select MAX(start_addr_m) as lol FROM road_address rm WHERE road_number=$roadNumber AND road_part_number=$roadPart AND
           rm.valid_from <= sysdate AND (rm.valid_to is null or rm.valid_to > sysdate) AND track_code in (0,1))  ra
           on r.START_ADDR_M=ra.lol
           WHERE r.road_number=$roadNumber AND r.road_part_number=$roadPart AND
           r.valid_from <= sysdate AND (r.valid_to is null or r.valid_to > sysdate) AND track_code in (0,1)"""
    Q.queryNA[(Long,Long,Long,Long, Long, Option[DateTime], Option[DateTime])](query).firstOption
  }

  def fetchByLinkIdToApi(linkIds: Set[Long], useLatestNetwork: Boolean = true, ignoreHistory: Boolean = true): List[RoadAddress] = {
    if (useLatestNetwork)
      getRoadAddresses(byLinkIdToApi(linkIds, ignoreHistory), withLatestNetwork = true)
    else
      getRoadAddresses(byLinkIdToApi(linkIds, ignoreHistory))
  }

  def fetchMultiSegmentLinkIds(roadNumber: Long) = {
    val query =
      s"""
        select ra.id, ra.road_number, ra.road_part_number, ra.road_type, ra.track_code,
        ra.discontinuity, ra.start_addr_m, ra.end_addr_m, ra.lrm_position_id,pos.link_id, pos.start_measure, pos.end_measure,
        pos.side_code, pos.adjusted_timestamp,
        ra.start_date, ra.end_date, ra.created_by, ra.valid_from, ra.CALIBRATION_POINTS, ra.floating, t.X, t.Y, t2.X, t2.Y, link_source, ra.ely, ra.terminated, ra.common_history_id
        from road_address ra cross join
        TABLE(SDO_UTIL.GETVERTICES(ra.geometry)) t cross join
        TABLE(SDO_UTIL.GETVERTICES(ra.geometry)) t2
        join lrm_position pos on ra.lrm_position_id = pos.id
        where link_id in (
        select pos.link_id
        from road_address ra
        join lrm_position pos on ra.lrm_position_id = pos.id
        where road_number = $roadNumber AND valid_from <= sysdate and
          (valid_to is null or valid_to > sysdate)
        GROUP BY link_id
        HAVING COUNT(*) > 1) AND
        road_number = $roadNumber AND valid_from <= sysdate and
          (valid_to is null or valid_to > sysdate)
      """
    queryList(query)
  }

  def fetchAllRoadAddressErrors(includesHistory: Boolean = false) = {

    val history = if(!includesHistory) s" where ra.end_date is null " else ""
    val query =
      s"""
        SELECT RA.ID, LRM.LINK_ID, RA.ROAD_NUMBER, RA.ROAD_PART_NUMBER, RE.ERROR_CODE, RA.ELY FROM ROAD_ADDRESS RA JOIN LRM_POSITION LRM
         ON LRM.ID = RA.LRM_POSITION_ID JOIN ROAD_NETWORK_ERRORS RE ON RE.ROAD_ADDRESS_ID = RA.ID $history
         ORDER BY RA.ELY, RA.ROAD_NUMBER, RA.ROAD_PART_NUMBER, RE.ERROR_CODE
      """
    Q.queryNA[(Long, Long, Long, Long, Int, Long)](query).list.map {
      case (id, linkId, roadNumber, roadPartNumber, errorCode, ely) =>
        AddressErrorDetails(id, linkId, roadNumber, roadPartNumber, AddressError.apply(errorCode), ely)}
  }

  def setSubsequentTermination(linkIds: Set[Long]): Unit = {
    val roadAddresses = fetchByLinkId(linkIds, true, true).filter(_.terminated == NoTermination)
    expireById(roadAddresses.map(_.id).toSet)
    create(roadAddresses.map(ra => ra.copy(id = NewRoadAddress, terminated = Subsequent)))
  }

  def isNotAvailableForProject(roadNumber: Long, roadPartNumber: Long, projectId: Long): Boolean = {
    val query =
      s"""
      SELECT 1 FROM dual WHERE EXISTS(select 1
         from project pro,
         road_address ra
         join lrm_position pos on ra.lrm_position_id = pos.id
         where  pro.id = $projectId AND road_number = $roadNumber AND road_part_number = $roadPartNumber AND
         (ra.START_DATE >= pro.START_DATE or ra.END_DATE > pro.START_DATE) AND
         ra.VALID_TO is null) OR EXISTS (
         SELECT 1 FROM project_reserved_road_part pro, road_address ra JOIN lrm_position pos ON ra.lrm_position_id = pos.id
          WHERE pro.project_id != $projectId AND pro.road_number = ra.road_number AND pro.road_part_number = ra.road_part_number
           AND pro.road_number = $roadNumber AND pro.road_part_number = $roadPartNumber AND ra.end_date IS NULL)"""
    Q.queryNA[Int](query).firstOption.nonEmpty
  }

  def fetchByRoad(roadNumber: Long, includeFloating: Boolean = false): List[RoadAddress] = {
    getRoadAddresses(byRoad(roadNumber, includeFloating))
  }

  def fetchAllCurrentRoads(options: RoadCheckOptions): List[RoadAddress] = {
    getRoadAddresses(allCurrentRoads(options))
  }

  def lockRoadAddressWriting: Unit = {
    sqlu"""LOCK TABLE road_address IN SHARE MODE""".execute
  }

  def getRoadAddressByEly(ely: Long): List[RoadAddress] = {
    getRoadAddresses(byEly(ely))
  }

}
