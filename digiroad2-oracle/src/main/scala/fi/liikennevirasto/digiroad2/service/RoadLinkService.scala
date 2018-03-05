package fi.liikennevirasto.digiroad2.service

import java.io.{File, FilenameFilter, IOException}
import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit
import java.util.{Date, Properties}

import com.github.tototoshi.slick.MySQLJodaSupport._
import fi.liikennevirasto.digiroad2.asset.Asset._
import fi.liikennevirasto.digiroad2.asset._
import fi.liikennevirasto.digiroad2.client.vvh._
import fi.liikennevirasto.digiroad2.dao.RoadLinkServiceDAO
import fi.liikennevirasto.digiroad2.linearasset.{RoadLink, RoadLinkProperties}
import fi.liikennevirasto.digiroad2.oracle.{MassQuery, OracleDatabase}
import fi.liikennevirasto.digiroad2.asset.CycleOrPedestrianPath
import fi.liikennevirasto.digiroad2.util.{Track, VVHSerializer}
import fi.liikennevirasto.digiroad2.{DigiroadEventBus, GeometryUtils}
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import org.slf4j.{Logger, LoggerFactory}
import slick.driver.JdbcDriver.backend.Database.dynamicSession
import slick.jdbc.StaticQuery.interpolation
import slick.jdbc.{GetResult, PositionedResult, StaticQuery => Q}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

case class IncompleteLink(linkId: Long, municipalityCode: Int, administrativeClass: AdministrativeClass)
case class RoadLinkChangeSet(adjustedRoadLinks: Seq[RoadLink], incompleteLinks: Seq[IncompleteLink])
case class ChangedVVHRoadlink(link: RoadLink, value: String, createdAt: Option[DateTime], changeType: String /*TODO create and use ChangeType case object*/)

sealed trait RoadLinkType {
  def value: Int
}

object RoadLinkType{
  val values = Set(NormalRoadLinkType, ComplementaryRoadLinkType, UnknownRoadLinkType, FloatingRoadLinkType)

  def apply(intValue: Int): RoadLinkType = {
    values.find(_.value == intValue).getOrElse(UnknownRoadLinkType)
  }

  case object UnknownRoadLinkType extends RoadLinkType { def value = 0 }
  case object NormalRoadLinkType extends RoadLinkType { def value = 1 }
  case object ComplementaryRoadLinkType extends RoadLinkType { def value = 3 }
  case object FloatingRoadLinkType extends RoadLinkType { def value = -1 }
  case object SuravageRoadLink extends RoadLinkType { def value = 4}
}

/**
  * This class performs operations related to road links. It uses VVHClient to get data from VVH Rest API.
  *
  * @param vvhClient
  * @param eventbus
  * @param vvhSerializer
  */
class RoadLinkService(val vvhClient: VVHClient, val eventbus: DigiroadEventBus, val vvhSerializer: VVHSerializer) {
  val logger: Logger = LoggerFactory.getLogger(getClass)

  def withDynTransaction[T](f: => T): T = OracleDatabase.withDynTransaction(f)
  def withDynSession[T](f: => T): T = OracleDatabase.withDynSession(f)

  implicit val getDateTime = new GetResult[DateTime] {
    def apply(r: PositionedResult): DateTime = {
      new DateTime(r.nextTimestamp())
    }
  }

  def getRoadLinksAndComplementaryFromVVH(linkId: Set[Long], newTransaction: Boolean = true): Seq[RoadLink] = {
    val vvhRoadLinks = fetchVVHRoadlinksAndComplementary(linkId)
    if (newTransaction)
      withDynTransaction {
        enrichRoadLinksFromVVH(vvhRoadLinks)
      }
    else
      enrichRoadLinksFromVVH(vvhRoadLinks)
  }

  /**
    * ATENTION Use this method always with transaction not with session
    * This method returns road links by link ids.
    *
    * @param linkIds
    * @return Road links
    */
  def getRoadLinksByLinkIdsFromVVH(linkIds: Set[Long], newTransaction: Boolean = true): Seq[RoadLink] = {
    val vvhRoadLinks = fetchVVHRoadlinks(linkIds)
    if (newTransaction)
      withDynTransaction {
        enrichRoadLinksFromVVH(vvhRoadLinks)
      }
    else
      enrichRoadLinksFromVVH(vvhRoadLinks)
  }

  def getRoadLinkByLinkIdFromVVH(linkId: Long, newTransaction: Boolean = true): Option[RoadLink] = getRoadLinksByLinkIdsFromVVH(Set(linkId), newTransaction: Boolean).headOption

  def getSuravageRoadLinksByLinkIdsFromVVH(linkIds: Set[Long], newTransaction: Boolean = true): Seq[RoadLink] = {
    val vvhSuravageLinks = fetchSuravageLinksByLinkIdsFromVVH(linkIds)
    if (newTransaction)
      withDynTransaction {
        enrichRoadLinksFromVVH(vvhSuravageLinks)
      }
    else
      enrichRoadLinksFromVVH(vvhSuravageLinks)
  }

  def getViiteRoadLinksByLinkIdsFromVVH(linkIds: Set[Long], newTransaction: Boolean = true, frozenTimeVVHAPIServiceEnabled:Boolean = false): Seq[RoadLink] = {
    val vvhRoadLinks = fetchVVHRoadlinks(linkIds, frozenTimeVVHAPIServiceEnabled)
    if (newTransaction)
      withDynTransaction {
        enrichRoadLinksFromVVH(vvhRoadLinks)
      }
    else
      enrichRoadLinksFromVVH(vvhRoadLinks)
  }

  /**
    * This method returns road links by municipality.
    *
    * @param municipality
    * @return Road links
    */
  def getRoadLinksFromVVH(municipality: Int): Seq[RoadLink] = {
    getCachedRoadLinksAndChanges(municipality)._1
  }

  private def getRoadNodesByMunicipality(municipality: Int): Seq[VVHRoadNodes] = {
    getCachedRoadNodes(municipality)
  }

  private def getRoadNodesFromVVHFuture(municipality: Int): Future[Seq[VVHRoadNodes]] = {
    Future(getRoadNodesByMunicipality(municipality))
  }

  /**
    * This method returns road links by bounding box and municipalities.
    *
    * @param bounds
    * @param municipalities
    * @return Road links
    */
  def getRoadLinksFromVVH(bounds: BoundingRectangle, municipalities: Set[Int] = Set()) : Seq[RoadLink] =
    getRoadLinksAndChangesFromVVH(bounds, municipalities)._1

  /**
    * This method returns "real" road links and "complementary" road links by bounding box and municipalities.
    *
    * @param bounds
    * @param municipalities
    * @return Road links
    */
  def getRoadLinksWithComplementaryFromVVH(bounds: BoundingRectangle, municipalities: Set[Int] = Set(), newTransaction: Boolean = true) : Seq[RoadLink] =
    getRoadLinksWithComplementaryAndChangesFromVVH(bounds, municipalities, newTransaction)._1

  /**
    * This method is utilized to find adjacent links of a road link.
    *
    * @param bounds
    * @param bounds2
    * @return Road links
    */
  def getRoadLinksFromVVH(bounds: BoundingRectangle, bounds2: BoundingRectangle) : Seq[RoadLink] =
    getRoadLinksAndChangesFromVVH(bounds, bounds2)._1

  /**
    * This method returns VVH road links by link ids.
    *
    * @param linkIds
    * @return VVHRoadLinks
    */
  def fetchVVHRoadlinks(linkIds: Set[Long], frozenTimeVVHAPIServiceEnabled:Boolean = false): Seq[VVHRoadlink] = {
    if (linkIds.nonEmpty) {if(frozenTimeVVHAPIServiceEnabled){vvhClient.frozenTimeRoadLinkData.fetchByLinkIds(linkIds)} else vvhClient.roadLinkData.fetchByLinkIds(linkIds) }
    else Seq.empty[VVHRoadlink]
  }

  def fetchVVHRoadlinksAndComplementary(linkIds: Set[Long]): Seq[VVHRoadlink] = {
    if (linkIds.nonEmpty) vvhClient.roadLinkData.fetchByLinkIds(linkIds) ++ vvhClient.complementaryData.fetchByLinkIds(linkIds)
    else Seq.empty[VVHRoadlink]
  }

  /**
    * This method returns VVH road links that had changed between two dates.
    *
    * @param since
    * @param until
    * @return VVHRoadLinks
    */
  private def fetchChangedVVHRoadlinksBetweenDates(since: DateTime, until: DateTime): Seq[VVHRoadlink] = {
    if ((since != null) || (until != null)) vvhClient.roadLinkData.fetchByChangesDates(since, until)
    else Seq.empty[VVHRoadlink]
  }

  /**
    * This method returns road links and change data by bounding box and municipalities.
    *
    * @param bounds
    * @param municipalities
    * @return Road links and change data
    */
  private def getRoadLinksAndChangesFromVVH(bounds: BoundingRectangle, municipalities: Set[Int] = Set()): (Seq[RoadLink], Seq[ChangeInfo]) = {
    val (changes, links) =
      Await.result(vvhClient.roadLinkChangeInfo.fetchByBoundsAndMunicipalitiesF(bounds, municipalities).zip(vvhClient.roadLinkData.fetchByMunicipalitiesAndBoundsF(bounds, municipalities)), atMost = Duration.Inf)
    withDynTransaction {
      (enrichRoadLinksFromVVH(links, changes), changes)
    }
  }

  def getRoadLinksAndChangesFromVVHWithFrozenTimeAPI(bounds: BoundingRectangle, municipalities: Set[Int] = Set(), frozenTimeVVHAPIServiceEnabled:Boolean = false): (Seq[RoadLink], Seq[ChangeInfo])= {
    if (frozenTimeVVHAPIServiceEnabled) {
      val (changes, links) =
        Await.result(Future(Seq.empty[ChangeInfo]).zip(vvhClient.frozenTimeRoadLinkData.fetchByMunicipalitiesAndBoundsF(bounds, municipalities)), atMost = Duration.Inf)
      withDynTransaction {
        (enrichRoadLinksFromVVH(links, changes), changes)
      }
    } else
      getRoadLinksAndChangesFromVVH(bounds,municipalities)
  }

  /**
    * This method returns "real" road links, "complementary" road links and change data by bounding box and municipalities.
    *
    * @param bounds
    * @param municipalities
    * @return Road links and change data
    */
  private def getRoadLinksWithComplementaryAndChangesFromVVH(bounds: BoundingRectangle, municipalities: Set[Int] = Set(), newTransaction:Boolean = true): (Seq[RoadLink], Seq[ChangeInfo])= {
    val fut = for{
      f1Result <- vvhClient.complementaryData.fetchWalkwaysByBoundsAndMunicipalitiesF(bounds, municipalities)
      f2Result <- vvhClient.roadLinkChangeInfo.fetchByBoundsAndMunicipalitiesF(bounds, municipalities)
      f3Result <- vvhClient.roadLinkData.fetchByMunicipalitiesAndBoundsF(bounds, municipalities)
    } yield (f1Result, f2Result, f3Result)

    val (complementaryLinks, changes, links) = Await.result(fut, Duration.Inf)

    if(newTransaction){
      withDynTransaction {
        (enrichRoadLinksFromVVH(links ++ complementaryLinks, changes), changes)
      }
    }
    else (enrichRoadLinksFromVVH(links ++ complementaryLinks, changes), changes)

  }

  private def reloadRoadLinksWithComplementaryAndChangesFromVVH(municipalities: Int): (Seq[RoadLink], Seq[ChangeInfo], Seq[RoadLink])= {
    val fut = for{
      f1Result <- vvhClient.complementaryData.fetchWalkwaysByMunicipalitiesF(municipalities)
      f2Result <- vvhClient.roadLinkChangeInfo.fetchByMunicipalityF(municipalities)
      f3Result <- vvhClient.roadLinkData.fetchByMunicipalityF(municipalities)
    } yield (f1Result, f2Result, f3Result)

    val (complementaryLinks, changes, links) = Await.result(fut, Duration.Inf)

    withDynTransaction {
      (enrichRoadLinksFromVVH(links, changes), changes, enrichRoadLinksFromVVH(complementaryLinks, changes))
    }
  }

  def getFrozenViiteRoadLinksAndChangesFromVVH(municipality: Int, frozenTimeVVHAPIServiceEnabled: Boolean =false ): (Seq[RoadLink], Seq[ChangeInfo])= {
    getCachedRoadLinksAndChanges(municipality)
  }

  def getRoadLinksWithComplementaryAndChangesFromVVH(municipality: Int): (Seq[RoadLink], Seq[ChangeInfo])= {
    getCachedRoadLinksWithComplementaryAndChanges(municipality)
  }

  /**
    * This method is utilized to find adjacent links of a road link.
    *
    * @param bounds
    * @param bounds2
    * @return Road links and change data
    */
  private def getRoadLinksAndChangesFromVVH(bounds: BoundingRectangle, bounds2: BoundingRectangle): (Seq[RoadLink], Seq[ChangeInfo])= {
    val links1F = vvhClient.roadLinkData.fetchByMunicipalitiesAndBoundsF(bounds, Set())
    val links2F = vvhClient.roadLinkData.fetchByMunicipalitiesAndBoundsF(bounds2, Set())
    val changeF = vvhClient.roadLinkChangeInfo.fetchByBoundsAndMunicipalitiesF(bounds, Set())
    val ((links, links2), changes) = Await.result(links1F.zip(links2F).zip(changeF), atMost = Duration.apply(60, TimeUnit.SECONDS))
    withDynTransaction {
      (enrichRoadLinksFromVVH(links ++ links2, changes), changes)
    }
  }

  /**
    * Returns incomplete links by municipalities (Incomplete link = road link with no functional class and link type saved in OTH).
    * Used by Digiroad2Api /roadLinks/incomplete GET endpoint.
    */
  def getIncompleteLinks(includedMunicipalities: Option[Set[Int]]): Map[String, Map[String, Seq[Long]]] = {
    case class IncompleteLink(linkId: Long, municipality: String, administrativeClass: String)
    def toIncompleteLink(x: (Long, String, Int)) = IncompleteLink(x._1, x._2, AdministrativeClass(x._3).toString)

    withDynSession {
      val optionalMunicipalities = includedMunicipalities.map(_.mkString(","))
      val incompleteLinksQuery = """
        select l.link_id, m.name_fi, l.administrative_class
        from incomplete_link l
        join municipality m on l.municipality_code = m.id
                                 """

      val sql = optionalMunicipalities match {
        case Some(municipalities) => incompleteLinksQuery + s" where l.municipality_code in ($municipalities)"
        case _ => incompleteLinksQuery
      }

      Q.queryNA[(Long, String, Int)](sql).list
        .map(toIncompleteLink)
        .groupBy(_.municipality)
        .mapValues { _.groupBy(_.administrativeClass)
          .mapValues(_.map(_.linkId)) }
    }
  }

  protected def setLinkProperty(table: String, column: String, vvhColumn: String, value: Int, linkId: Long, username: Option[String],
                                optionalVVHValue: Option[Int] = None, latestModifiedAt: Option[String],
                                latestModifiedBy: Option[String], mmlId: Option[Long]): Unit = {
    val optionalExistingValue: Option[Int] = RoadLinkServiceDAO.getLinkProperty(table, column, linkId)
    (optionalExistingValue, optionalVVHValue) match {
      case (Some(existingValue), _) =>
        table match{
          case "administrative_class" => RoadLinkServiceDAO.updateExistingAdministrativeClass(table, column, vvhColumn, linkId, username, existingValue, value, mmlId, optionalVVHValue)
          case _ => RoadLinkServiceDAO.updateExistingLinkPropertyRow(table, column, linkId, username, existingValue, value)
        }
      case (None, None) =>
        insertLinkProperty(optionalExistingValue, optionalVVHValue, table, column, vvhColumn, linkId, username, value, latestModifiedAt, latestModifiedBy, mmlId)

      case (None, Some(vvhValue)) =>
        if (vvhValue != value) // only save if it overrides VVH provided value
          insertLinkProperty(optionalExistingValue, optionalVVHValue, table, column, vvhColumn, linkId, username, value, latestModifiedAt, latestModifiedBy, mmlId)
    }
  }

  private def insertLinkProperty(optionalExistingValue: Option[Int], optionalVVHValue: Option[Int], table: String,
                                 column: String, vvhColumn: String, linkId: Long, username: Option[String], value: Int, latestModifiedAt: Option[String],
                                 latestModifiedBy: Option[String], mmlId: Option[Long]) = {
    if (latestModifiedAt.isEmpty) {
      table match{
        case "administrative_class" => RoadLinkServiceDAO.insertNewAdministrativeClass(table, column, vvhColumn, linkId, username, value, mmlId, optionalVVHValue)
        case _ =>  RoadLinkServiceDAO.insertNewLinkProperty(table, column, linkId, username, value)
      }
    } else{
      try {
        var parsedDate = ""
        if (latestModifiedAt.get.matches("^\\d\\d\\.\\d\\d\\.\\d\\d\\d\\d.*")) {
          // Finnish date format
          parsedDate = DateTimePropertyFormat.parseDateTime(latestModifiedAt.get).toString()
        } else {
          parsedDate = DateTime.parse(latestModifiedAt.get).toString(ISODateTimeFormat.dateTime())
        }
        sqlu"""insert into #$table (id, link_id, #$column, modified_date, modified_by)
                 select primary_key_seq.nextval, $linkId, $value,
                 to_timestamp_tz($parsedDate, 'YYYY-MM-DD"T"HH24:MI:SS.ff3"+"TZH:TZM'), $latestModifiedBy
                 from dual
                 where not exists (select * from #$table where link_id = $linkId)""".execute
      } catch {
        case e: Exception =>
          println("ERR! -> table " + table + " (" + linkId + ", " + value + "): mod timestamp = " + latestModifiedAt.getOrElse("null"))
          throw e
      }
    }
  }


  private def fetchOverrides(idTableName: String): Map[Long, (Option[(Long, Int, DateTime, String)],
    Option[(Long, Int, DateTime, String)], Option[(Long, Int, DateTime, String)], Option[(Long, Int, DateTime, String)])] = {
    sql"""select i.id, t.link_id, t.traffic_direction, t.modified_date, t.modified_by,
          f.link_id, f.functional_class, f.modified_date, f.modified_by,
          l.link_id, l.link_type, l.modified_date, l.modified_by,
          a.link_id, a.administrative_class, a.created_date, a.created_by
            from #$idTableName i
            left join traffic_direction t on i.id = t.link_id
            left join functional_class f on i.id = f.link_id
            left join link_type l on i.id = l.link_id
            left join administrative_class a on i.id = a.link_id where a.valid_to is null
      """.as[(Long, Option[Long], Option[Int], Option[DateTime], Option[String],
      Option[Long], Option[Int], Option[DateTime], Option[String],
      Option[Long], Option[Int], Option[DateTime], Option[String],
      Option[Long], Option[Int], Option[DateTime], Option[String])].list.map(row =>
    {
      val td = (row._2, row._3, row._4, row._5) match {
        case (Some(linkId), Some(dir), Some(modDate), Some(modBy)) => Option((linkId, dir, modDate, modBy))
        case _ => None
      }
      val fc = (row._6, row._7, row._8, row._9) match {
        case (Some(linkId), Some(dir), Some(modDate), Some(modBy)) => Option((linkId, dir, modDate, modBy))
        case _ => None
      }
      val lt = (row._10, row._11, row._12, row._13) match {
        case (Some(linkId), Some(dir), Some(modDate), Some(modBy)) => Option((linkId, dir, modDate, modBy))
        case _ => None
      }
      val ac = (row._14, row._15, row._16, row._17) match{
        case (Some(linkId), Some(value), Some(createdDate), Some(createdBy)) => Option((linkId, value, createdDate, createdBy))
        case _ => None
      }
      row._1 ->(td, fc, lt, ac)
    }
    ).toMap
  }

  def getViiteRoadLinksHistoryFromVVH(roadAddressesLinkIds: Set[Long]): Seq[VVHHistoryRoadLink] = {
    if (roadAddressesLinkIds.nonEmpty) {
      val historyData = Await.result(vvhClient.historyData.fetchVVHRoadLinkByLinkIdsF(roadAddressesLinkIds), atMost = Duration.Inf)
      val groupedData = historyData.groupBy(_.linkId)
      groupedData.mapValues(_.maxBy(_.endDate)).values.toSeq
    } else
      Nil
  }

  def getViiteCurrentAndHistoryRoadLinksFromVVH(linkIds: Set[Long],
                                                useFrozenVVHLinks: Boolean = false): (Seq[RoadLink], Seq[VVHHistoryRoadLink]) = {
    val fut = for{
      f1Result <- vvhClient.historyData.fetchVVHRoadLinkByLinkIdsF(linkIds)
      f2Result <- if(useFrozenVVHLinks)vvhClient.frozenTimeRoadLinkData.fetchByLinkIdsF(linkIds) else {vvhClient.roadLinkData.fetchByLinkIdsF(linkIds)}
      f3Result <- vvhClient.complementaryData.fetchByLinkIdsF(linkIds)
    } yield (f1Result, f2Result, f3Result)

    val (historyData, currentData, complementaryData) = Await.result(fut, Duration.Inf)
    val uniqueHistoryData = historyData.groupBy(_.linkId).mapValues(_.maxBy(_.endDate)).values.toSeq

    if (OracleDatabase.isWithinSession)
      (enrichRoadLinksFromVVH(currentData ++ complementaryData, Seq()), uniqueHistoryData)
    else {
      withDynTransaction {
        (enrichRoadLinksFromVVH(currentData ++ complementaryData, Seq()), uniqueHistoryData)
      }
    }
  }

  /**
    * Returns road links and change data from VVH by bounding box and road numbers and municipalities. Used by RoadLinkService.getRoadLinksFromVVH and SpeedLimitService.get.
    */
  private def getViiteRoadLinksAndChangesFromVVH(bounds: BoundingRectangle, roadNumbers: Seq[(Int, Int)],
                                         municipalities: Set[Int] = Set(), everything: Boolean,
                                         publicRoads: Boolean): (Seq[RoadLink], Seq[ChangeInfo])= {
    val (changes, links) = Await.result(vvhClient.roadLinkChangeInfo.fetchByBoundsAndMunicipalitiesF(bounds, municipalities)
      .zip(if (everything) {
        vvhClient.roadLinkData.fetchByMunicipalitiesAndBoundsF(bounds, municipalities)
      } else {
        vvhClient.roadLinkData.fetchByRoadNumbersBoundsAndMunicipalitiesF(bounds, municipalities, roadNumbers, publicRoads)
      }), atMost = Duration.Inf)

    withDynTransaction {
      (enrichRoadLinksFromVVH(links, changes), changes)
    }
  }

  /**
    * Returns road links without change data for Viite from VVH by bounding box and road numbers and municipalities.
    */
  private def getViiteRoadLinksFromVVH(bounds: BoundingRectangle, roadNumbers: Seq[(Int, Int)],
                               municipalities: Set[Int] = Set(),
                               publicRoads: Boolean, frozenTimeVVHAPIServiceEnabled:Boolean): Seq[RoadLink] = {
    val links = Await.result(

      if(frozenTimeVVHAPIServiceEnabled)
        vvhClient.frozenTimeRoadLinkData.fetchByRoadNumbersBoundsAndMunicipalitiesF(bounds, municipalities, roadNumbers, publicRoads)
      else
        vvhClient.roadLinkData.fetchByRoadNumbersBoundsAndMunicipalitiesF(bounds, municipalities, roadNumbers, publicRoads),
      atMost = Duration.Inf)

    withDynTransaction {
      (enrichRoadLinksFromVVH(links, Seq()), Seq())._1
    }
  }

  /**
    * Returns the road links from VVH by municipality.
    *
    * @param municipality A integer, representative of the municipality Id.
    */
  def getViiteRoadLinksFromVVHByMunicipality(municipality: Int, frozenTimeVVHAPIServiceEnabled:Boolean =false): Seq[RoadLink] = {
    val links = if(frozenTimeVVHAPIServiceEnabled){vvhClient.frozenTimeRoadLinkData.fetchByMunicipality(municipality)} else vvhClient.roadLinkData.fetchByMunicipality(municipality)
    withDynTransaction {
      (enrichRoadLinksFromVVH(links, Seq()), Seq())._1
    }
  }

  /**
    * Returns road links and change data from VVH by bounding box and road numbers and municipalities. Used by RoadLinkService.getRoadLinksFromVVH and SpeedLimitService.get.
    */
  private def getViiteRoadLinksWithoutChangesFromVVH(linkIds: Set[Long], municipalities: Set[Int] = Set()): (Seq[RoadLink], Seq[ChangeInfo])= {
    (getViiteRoadLinksByLinkIdsFromVVH(linkIds), Seq())
  }

  def getViiteRoadLinksFromVVH(bounds: BoundingRectangle, roadNumbers: Seq[(Int, Int)], municipalities: Set[Int],
                               everything: Boolean, publicRoads: Boolean, frozenTimeVVHAPIServiceEnabled:Boolean) : Seq[RoadLink] =
    if (bounds.area >= 1E6 || frozenTimeVVHAPIServiceEnabled)
      getViiteRoadLinksFromVVH(bounds, roadNumbers, municipalities, publicRoads,frozenTimeVVHAPIServiceEnabled)
    else
      getViiteRoadLinksAndChangesFromVVH(bounds, roadNumbers, municipalities, everything, publicRoads)._1

  def getViiteRoadPartsFromVVH(linkIds: Set[Long], municipalities: Set[Int] = Set()) : Seq[RoadLink] =
    getViiteRoadLinksWithoutChangesFromVVH(linkIds, municipalities)._1

  def getChangeInfoFromVVHF(bounds: BoundingRectangle, municipalities: Set[Int]): Future[Seq[ChangeInfo]] ={
    vvhClient.roadLinkChangeInfo.fetchByBoundsAndMunicipalitiesF(bounds, municipalities)
  }

  def getChangeInfoFromVVHF(linkIds: Set[Long]): Future[Seq[ChangeInfo]] ={
    vvhClient.roadLinkChangeInfo.fetchByLinkIdsF(linkIds)
  }

  private def reloadRoadNodesFromVVH(municipality: Int): (Seq[VVHRoadNodes])= {
    vvhClient.roadNodesData.fetchByMunicipality(municipality)
  }

  protected def removeIncompleteness(linkId: Long): Unit = {
    sqlu"""delete from incomplete_link where link_id = $linkId""".execute
  }

  /**
    * Updates road link data in OTH db. Used by Digiroad2Context LinkPropertyUpdater Akka actor.
    */
  def updateRoadLinkChanges(roadLinkChangeSet: RoadLinkChangeSet): Unit = {
    updateAutoGeneratedProperties(roadLinkChangeSet.adjustedRoadLinks)
    updateIncompleteLinks(roadLinkChangeSet.incompleteLinks)
  }

  /**
    * Updates road link autogenerated properties (functional class, link type and traffic direction). Used by RoadLinkService.updateRoadLinkChanges.
    */
  private def updateAutoGeneratedProperties(adjustedRoadLinks: Seq[RoadLink]) {
    def createUsernameForAutogenerated(modifiedBy: Option[String]): Option[String] =  {
      modifiedBy match {
        case Some("automatic_generation") => modifiedBy
        case _ => None
      }
    }
    def updateProperties(vvhRoadLinks: Seq[VVHRoadlink])(roadLink: RoadLink) = {
      val vvhRoadLink = vvhRoadLinks.find(_.linkId == roadLink.linkId)
      val vvhTrafficDirection = vvhRoadLink.map(v => v.trafficDirection.value)
      // Separate auto-generated links from change info links: username should be empty for change info links
      val username = createUsernameForAutogenerated(roadLink.modifiedBy)

      if (roadLink.trafficDirection != TrafficDirection.UnknownDirection)  setLinkProperty("traffic_direction", "traffic_direction", "", roadLink.trafficDirection.value, roadLink.linkId, None, vvhTrafficDirection, roadLink.modifiedAt, roadLink.modifiedBy, None)
      if (roadLink.functionalClass != FunctionalClass.Unknown) setLinkProperty("functional_class", "functional_class", "", roadLink.functionalClass, roadLink.linkId, username, None, roadLink.modifiedAt, roadLink.modifiedBy, None)
      if (roadLink.linkType != UnknownLinkType) setLinkProperty("link_type", "link_type", "", roadLink.linkType.value, roadLink.linkId, username, None, roadLink.modifiedAt, roadLink.modifiedBy, None)
    }
    val vvhRoadLinks = vvhClient.roadLinkData.fetchByLinkIds(adjustedRoadLinks.map(_.linkId).toSet)
    withDynTransaction {
      adjustedRoadLinks.foreach(updateProperties(vvhRoadLinks))
      adjustedRoadLinks.foreach(link =>
        if (link.functionalClass != FunctionalClass.Unknown && link.linkType != UnknownLinkType) removeIncompleteness(link.linkId)
      )
    }
  }

  /**
    * Updates incomplete road link list (incomplete = functional class or link type missing). Used by RoadLinkService.updateRoadLinkChanges.
    */
  protected def updateIncompleteLinks(incompleteLinks: Seq[IncompleteLink]): Unit = {
    def setIncompleteness(incompleteLink: IncompleteLink) {
      withDynTransaction {
        sqlu"""insert into incomplete_link(id, link_id, municipality_code, administrative_class)
                 select primary_key_seq.nextval, ${incompleteLink.linkId}, ${incompleteLink.municipalityCode}, ${incompleteLink.administrativeClass.value} from dual
                 where not exists (select * from incomplete_link where link_id = ${incompleteLink.linkId})""".execute
      }
    }
    incompleteLinks.foreach(setIncompleteness)
  }

  /**
    * Returns value when all given values are the same. Used by RoadLinkService.fillIncompleteLinksWithPreviousLinkData.
    */
  private def useValueWhenAllEqual[T](values: Seq[T]): Option[T] = {
    if (values.nonEmpty && values.forall(_ == values.head))
      Some(values.head)
    else
      None
  }

  private def getLatestModification[T](values: Map[Option[String], Option [String]]) = {
    if (values.nonEmpty)
      Some(values.reduce(calculateLatestDate))
    else
      None
  }

  private def calculateLatestDate(stringOption1: (Option[String], Option[String]), stringOption2: (Option[String], Option[String])): (Option[String], Option[String]) = {
    val date1 = convertStringToDate(stringOption1._1)
    val date2 = convertStringToDate(stringOption2._1)
    (date1, date2) match {
      case (Some(d1), Some(d2)) =>
        if (d1.after(d2))
          stringOption1
        else
          stringOption2
      case (Some(_), None) => stringOption1
      case (None, Some(_)) => stringOption2
      case (None, None) => (None, None)
    }
  }

  private def convertStringToDate(str: Option[String]): Option[Date] = {
    if (str.exists(_.trim.nonEmpty))
      Some(new SimpleDateFormat("dd.MM.yyyy hh:mm:ss").parse(str.get))
    else
      None
  }

  /**
    *  Fills incomplete road links with the previous link information.
    *  Used by ROadLinkService.enrichRoadLinksFromVVH.
    */
  private def fillIncompleteLinksWithPreviousLinkData(incompleteLinks: Seq[RoadLink], changes: Seq[ChangeInfo]): (Seq[RoadLink], Seq[RoadLink]) = {
    val oldRoadLinkProperties = getOldRoadLinkPropertiesForChanges(changes)
    incompleteLinks.map { incompleteLink =>
      val oldIdsForIncompleteLink = changes.filter(_.newId == Option(incompleteLink.linkId)).flatMap(_.oldId)
      val oldPropertiesForIncompleteLink = oldRoadLinkProperties.filter(oldLink => oldIdsForIncompleteLink.contains(oldLink.linkId))
      val newFunctionalClass = incompleteLink.functionalClass match {
        case FunctionalClass.Unknown =>  useValueWhenAllEqual(oldPropertiesForIncompleteLink.map(_.functionalClass)).getOrElse(FunctionalClass.Unknown)
        case _ => incompleteLink.functionalClass
      }
      val newLinkType = incompleteLink.linkType match {
        case UnknownLinkType => useValueWhenAllEqual(oldPropertiesForIncompleteLink.map(_.linkType)).getOrElse(UnknownLinkType)
        case _ => incompleteLink.linkType
      }
      val modifications = (oldPropertiesForIncompleteLink.map(_.modifiedAt) zip oldPropertiesForIncompleteLink.map(_.modifiedBy)).toMap
      val modificationsWithVVHModification = modifications ++ Map(incompleteLink.modifiedAt -> incompleteLink.modifiedBy)
      val (newModifiedAt, newModifiedBy) = getLatestModification(modificationsWithVVHModification).getOrElse(incompleteLink.modifiedAt, incompleteLink.modifiedBy)
      val previousDirection = useValueWhenAllEqual(oldPropertiesForIncompleteLink.map(_.trafficDirection))

      incompleteLink.copy(
        functionalClass  = newFunctionalClass,
        linkType          = newLinkType,
        trafficDirection  =  previousDirection match
        { case Some(TrafficDirection.UnknownDirection) => incompleteLink.trafficDirection
          case None => incompleteLink.trafficDirection
          case _ => previousDirection.get
        },
        modifiedAt = newModifiedAt,
        modifiedBy = newModifiedBy)
    }.partition(isComplete)
  }

  /**
    * Checks if road link is complete (has both functional class and link type in OTH).
    * Used by RoadLinkService.fillIncompleteLinksWithPreviousLinkData and RoadLinkService.isIncomplete.
    */
  private def isComplete(roadLink: RoadLink): Boolean = {
    roadLink.linkSource != LinkGeomSource.NormalLinkInterface ||
      roadLink.functionalClass != FunctionalClass.Unknown && roadLink.linkType.value != UnknownLinkType.value
  }

  /**
    * Checks if road link is not complete. Used by RoadLinkService.enrichRoadLinksFromVVH.
    */
  private def isIncomplete(roadLink: RoadLink): Boolean = !isComplete(roadLink)

  /**
    * Checks if road link is partially complete (has functional class OR link type but not both). Used by RoadLinkService.enrichRoadLinksFromVVH.
    */
  private def isPartiallyIncomplete(roadLink: RoadLink): Boolean = {
    val onlyFunctionalClassIsSet = roadLink.functionalClass != FunctionalClass.Unknown && roadLink.linkType.value == UnknownLinkType.value
    val onlyLinkTypeIsSet = roadLink.functionalClass == FunctionalClass.Unknown && roadLink.linkType.value != UnknownLinkType.value
    onlyFunctionalClassIsSet || onlyLinkTypeIsSet
  }

  /**
    * This method performs formatting operations to given vvh road links:
    * - auto-generation of functional class and link type by feature class
    * - information transfer from old link to new link from change data
    * It also passes updated links and incomplete links to be saved to db by actor.
    *
    * @param vvhRoadLinks
    * @param changes
    * @return Road links
    */
  protected def enrichRoadLinksFromVVH(vvhRoadLinks: Seq[VVHRoadlink], changes: Seq[ChangeInfo] = Nil): Seq[RoadLink] = {
    def autoGenerateProperties(roadLink: RoadLink): RoadLink = {
      val vvhRoadLink = vvhRoadLinks.find(_.linkId == roadLink.linkId)
      vvhRoadLink.get.featureClass match {
        case FeatureClass.TractorRoad => roadLink.copy(functionalClass = 7, linkType = TractorRoad, modifiedBy = Some("automatic_generation"), modifiedAt = Some(DateTimePropertyFormat.print(DateTime.now())))
        case FeatureClass.DrivePath => roadLink.copy(functionalClass = 6, linkType = SingleCarriageway, modifiedBy = Some("automatic_generation"), modifiedAt = Some(DateTimePropertyFormat.print(DateTime.now())))
        case FeatureClass.CycleOrPedestrianPath => roadLink.copy(functionalClass = 8, linkType = CycleOrPedestrianPath, modifiedBy = Some("automatic_generation"), modifiedAt = Some(DateTimePropertyFormat.print(DateTime.now())))
        case _ => roadLink //similar logic used in RoadAddressBuilder
      }
    }
    def toIncompleteLink(roadLink: RoadLink): IncompleteLink = {
      val vvhRoadLink = vvhRoadLinks.find(_.linkId == roadLink.linkId)
      IncompleteLink(roadLink.linkId, vvhRoadLink.get.municipalityCode, roadLink.administrativeClass)
    }

    def canBeAutoGenerated(roadLink: RoadLink): Boolean = {
      vvhRoadLinks.find(_.linkId == roadLink.linkId).get.featureClass match {
        case FeatureClass.AllOthers => false
        case _ => true
      }
    }

    val roadLinkDataByLinkId: Seq[RoadLink] = getRoadLinkDataByLinkIds(vvhRoadLinks)
    val (incompleteLinks, completeLinks) = roadLinkDataByLinkId.partition(isIncomplete)
    val (linksToAutoGenerate, incompleteOtherLinks) = incompleteLinks.partition(canBeAutoGenerated)
    val autoGeneratedLinks = linksToAutoGenerate.map(autoGenerateProperties)
    val (changedLinks, stillIncompleteLinks) = fillIncompleteLinksWithPreviousLinkData(incompleteOtherLinks, changes)
    val changedPartiallyIncompleteLinks = stillIncompleteLinks.filter(isPartiallyIncomplete)
    val stillIncompleteLinksInUse = stillIncompleteLinks.filter(_.constructionType == ConstructionType.InUse)

    eventbus.publish("linkProperties:changed",
      RoadLinkChangeSet(autoGeneratedLinks ++ changedLinks ++ changedPartiallyIncompleteLinks, stillIncompleteLinksInUse.map(toIncompleteLink)))

    completeLinks ++ autoGeneratedLinks ++ changedLinks ++ stillIncompleteLinks
  }

  /**
    * Uses old road link ids from change data to fetch their OTH overridden properties from db.
    * Used by RoadLinkSErvice.fillIncompleteLinksWithPreviousLinkData.
    */
  private def getOldRoadLinkPropertiesForChanges(changes: Seq[ChangeInfo]): Seq[RoadLinkProperties] = {
    val oldLinkIds = changes.flatMap(_.oldId)
    val propertyRows = fetchRoadLinkPropertyRows(oldLinkIds.toSet)

    oldLinkIds.map { linkId =>
      val latestModification = propertyRows.latestModifications(linkId)
      val (modifiedAt, modifiedBy) = (latestModification.map(_._1), latestModification.map(_._2))

      RoadLinkProperties(linkId,
        propertyRows.functionalClassValue(linkId),
        propertyRows.linkTypeValue(linkId),
        propertyRows.trafficDirectionValue(linkId).getOrElse(TrafficDirection.UnknownDirection),
        propertyRows.administrativeClassValue(linkId).getOrElse(Unknown),
        modifiedAt.map(DateTimePropertyFormat.print),
        modifiedBy)
    }
  }

  /**
    * Passes VVH road links to adjustedRoadLinks to get road links. Used by RoadLinkService.enrichRoadLinksFromVVH.
    */
  private def getRoadLinkDataByLinkIds(vvhRoadLinks: Seq[VVHRoadlink]): Seq[RoadLink] = {
    adjustedRoadLinks(vvhRoadLinks)
  }

  private def adjustedRoadLinks(vvhRoadlinks: Seq[VVHRoadlink]): Seq[RoadLink] = {
    val propertyRows = fetchRoadLinkPropertyRows(vvhRoadlinks.map(_.linkId).toSet)

    vvhRoadlinks.map { link =>
      val latestModification = propertyRows.latestModifications(link.linkId, link.modifiedAt.map(at => (at, "vvh_modified")))
      val (modifiedAt, modifiedBy) = (latestModification.map(_._1), latestModification.map(_._2))

      RoadLink(link.linkId, link.geometry,
        GeometryUtils.geometryLength(link.geometry),
        propertyRows.administrativeClassValue(link.linkId).getOrElse(link.administrativeClass),
        propertyRows.functionalClassValue(link.linkId),
        propertyRows.trafficDirectionValue(link.linkId).getOrElse(link.trafficDirection),
        propertyRows.linkTypeValue(link.linkId),
        modifiedAt.map(DateTimePropertyFormat.print),
        modifiedBy, link.attributes, link.constructionType, link.linkSource)
    }
  }

  private def fetchRoadLinkPropertyRows(linkIds: Set[Long]): RoadLinkPropertyRows = {
    def cleanMap(parameterMap: Map[Long, (Option[(Long, Int, DateTime, String)])]): Map[RoadLinkId, RoadLinkPropertyRow] = {
      parameterMap.filter(i => i._2.nonEmpty).mapValues(i => i.get)
    }
    def splitMap(parameterMap: Map[Long, (Option[(Long, Int, DateTime, String)],
      Option[(Long, Int, DateTime, String)], Option[(Long, Int, DateTime, String)],
      Option[(Long, Int, DateTime, String)] )]) = {
      (cleanMap(parameterMap.map(i => i._1 -> i._2._1)),
        cleanMap(parameterMap.map(i => i._1 -> i._2._2)),
        cleanMap(parameterMap.map(i => i._1 -> i._2._3)),
        cleanMap(parameterMap.map(i => i._1 -> i._2._4)))
    }
    MassQuery.withIds(linkIds) {
      idTableName =>
        val (td, fc, lt, ac) = splitMap(fetchOverrides(idTableName))
        RoadLinkPropertyRows(td, fc, lt, ac)
    }
  }

  type RoadLinkId = Long
  type RoadLinkPropertyRow = (Long, Int, DateTime, String)

  case class RoadLinkPropertyRows(trafficDirectionRowsByLinkId: Map[RoadLinkId, RoadLinkPropertyRow],
                                  functionalClassRowsByLinkId: Map[RoadLinkId, RoadLinkPropertyRow],
                                  linkTypeRowsByLinkId: Map[RoadLinkId, RoadLinkPropertyRow],
                                  administrativeClassRowsByLinkId: Map[RoadLinkId, RoadLinkPropertyRow]) {

    def functionalClassValue(linkId: Long): Int = {
      val functionalClassRowOption = functionalClassRowsByLinkId.get(linkId)
      functionalClassRowOption.map(_._2).getOrElse(FunctionalClass.Unknown)
    }

    def linkTypeValue(linkId: Long): LinkType = {
      val linkTypeRowOption = linkTypeRowsByLinkId.get(linkId)
      linkTypeRowOption.map(linkTypeRow => LinkType(linkTypeRow._2)).getOrElse(UnknownLinkType)
    }

    def trafficDirectionValue(linkId: Long): Option[TrafficDirection] = {
      val trafficDirectionRowOption = trafficDirectionRowsByLinkId.get(linkId)
      trafficDirectionRowOption.map(trafficDirectionRow => TrafficDirection(trafficDirectionRow._2))
    }

    def administrativeClassValue(linkId: Long): Option[AdministrativeClass] = {
      val administrativeRowOption = administrativeClassRowsByLinkId.get(linkId)
      administrativeRowOption.map( ac => AdministrativeClass.apply(ac._2))
    }

    def latestModifications(linkId: Long, optionalModification: Option[(DateTime, String)] = None): Option[(DateTime, String)] = {
      val functionalClassRowOption = functionalClassRowsByLinkId.get(linkId)
      val linkTypeRowOption = linkTypeRowsByLinkId.get(linkId)
      val trafficDirectionRowOption = trafficDirectionRowsByLinkId.get(linkId)
      val administrativeRowOption = administrativeClassRowsByLinkId.get(linkId)

      val modifications = List(functionalClassRowOption, trafficDirectionRowOption, linkTypeRowOption, administrativeRowOption).map {
        case Some((_, _, at, by)) => Some((at, by))
        case _ => None
      } :+ optionalModification
      modifications.reduce(calculateLatestModifications)
    }

    private def calculateLatestModifications(a: Option[(DateTime, String)], b: Option[(DateTime, String)]) = {
      (a, b) match {
        case (Some((firstModifiedAt, firstModifiedBy)), Some((secondModifiedAt, secondModifiedBy))) =>
          if (firstModifiedAt.isAfter(secondModifiedAt))
            Some((firstModifiedAt, firstModifiedBy))
          else
            Some((secondModifiedAt, secondModifiedBy))
        case (Some((firstModifiedAt, firstModifiedBy)), None) => Some((firstModifiedAt, firstModifiedBy))
        case (None, Some((secondModifiedAt, secondModifiedBy))) => Some((secondModifiedAt, secondModifiedBy))
        case (None, None) => None
      }
    }
  }

  private val cacheDirectory = {
    val properties = new Properties()
    properties.load(getClass.getResourceAsStream("/digiroad2.properties"))
    properties.getProperty("digiroad2.cache.directory", "/tmp/digiroad.cache")
  }

  private def getCacheDirectory: Option[File] = {
    val file = new File(cacheDirectory)
    try {
      if ((file.exists || file.mkdir()) && file.isDirectory) {
        return Option(file)
      } else {
        logger.error("Unable to create cache directory " + cacheDirectory)
      }
    } catch {
      case ex: SecurityException =>
        logger.error("Unable to create cache directory due to security", ex)
      case ex: IOException =>
        logger.error("Unable to create cache directory due to I/O error", ex)
    }
    None
  }

  private val geometryCacheFileNames = "geom_%d_%d.cached"
  private val changeCacheFileNames = "changes_%d_%d.cached"
  private val nodeCacheFileNames = "nodes_%d_%d.cached"
  private val geometryCacheStartsMatch = "geom_%d_"
  private val changeCacheStartsMatch = "changes_%d_"
  private val nodeCacheStartsMatch = "nodes_%d_"
  private val allCacheEndsMatch = ".cached"
  private val complementaryCacheFileNames = "complementary_%d_%d.cached"
  private val complementaryCacheStartsMatch = "complementary_%d_"

  private def deleteOldNodeCacheFiles(municipalityCode: Int, dir: Option[File], maxAge: Long) = {
    val oldNodeCacheFiles = dir.map(cacheDir => cacheDir.listFiles(new FilenameFilter {
      override def accept(dir: File, name: String): Boolean = {
        name.startsWith(nodeCacheStartsMatch.format(municipalityCode))
      }
    }).filter(f => f.lastModified() + maxAge < System.currentTimeMillis))
    oldNodeCacheFiles.getOrElse(Array()).foreach(f =>
      try {
        f.delete()
      } catch {
        case ex: Exception => logger.warn("Unable to delete old node cache file " + f.toPath, ex)
      }
    )
  }

  private def deleteOldCacheFiles(municipalityCode: Int, dir: Option[File], maxAge: Long) = {
    val oldCacheFiles = dir.map(cacheDir => cacheDir.listFiles(new FilenameFilter {
      override def accept(dir: File, name: String): Boolean = {
        name.startsWith(geometryCacheStartsMatch.format(municipalityCode))
      }
    }).filter(f => f.lastModified() + maxAge < System.currentTimeMillis))
    oldCacheFiles.getOrElse(Array()).foreach(f =>
      try {
        f.delete()
      } catch {
        case ex: Exception => logger.warn("Unable to delete old Geometry cache file " + f.toPath, ex)
      }
    )
    val oldChangesCacheFiles = dir.map(cacheDir => cacheDir.listFiles(new FilenameFilter {
      override def accept(dir: File, name: String): Boolean = {
        name.startsWith(changeCacheStartsMatch.format(municipalityCode))
      }
    }).filter(f => f.lastModified() + maxAge < System.currentTimeMillis))
    oldChangesCacheFiles.getOrElse(Array()).foreach(f =>
      try {
        f.delete()
      } catch {
        case ex: Exception => logger.warn("Unable to delete old change cache file " + f.toPath, ex)
      }
    )
    val oldCompCacheFiles = dir.map(cacheDir => cacheDir.listFiles(new FilenameFilter {
      override def accept(dir: File, name: String): Boolean = {
        name.startsWith(complementaryCacheStartsMatch.format(municipalityCode))
      }
    }).filter(f => f.lastModified() + maxAge < System.currentTimeMillis))
    oldCompCacheFiles.getOrElse(Array()).foreach(f =>
      try {
        f.delete()
      } catch {
        case ex: Exception => logger.warn("Unable to delete old Complementary cache file " + f.toPath, ex)
      }
    )
  }

  private def getCacheWithComplementaryFiles(municipalityCode: Int, dir: Option[File]): (Option[(File, File, File)]) = {
    val twentyHours = 20L * 60 * 60 * 1000
    deleteOldCacheFiles(municipalityCode, dir, twentyHours)

    val cachedGeometryFile = dir.map(cacheDir => cacheDir.listFiles(new FilenameFilter {
      override def accept(dir: File, name: String): Boolean = {
        name.startsWith(geometryCacheStartsMatch.format(municipalityCode))
      }
    }).filter(f => f.lastModified() + twentyHours > System.currentTimeMillis))

    val cachedChangesFile = dir.map(cacheDir => cacheDir.listFiles(new FilenameFilter {
      override def accept(dir: File, name: String): Boolean = {
        name.startsWith(changeCacheStartsMatch.format(municipalityCode))
      }
    }).filter(f => f.lastModified() + twentyHours > System.currentTimeMillis))

    val cachedComplementaryFile = dir.map(cacheDir => cacheDir.listFiles(new FilenameFilter {
      override def accept(dir: File, name: String): Boolean = {
        name.startsWith(complementaryCacheStartsMatch.format(municipalityCode))
      }
    }).filter(f => f.lastModified() + twentyHours > System.currentTimeMillis))

    if (cachedGeometryFile.nonEmpty && cachedGeometryFile.get.nonEmpty && cachedGeometryFile.get.head.canRead &&
      cachedChangesFile.nonEmpty && cachedChangesFile.get.nonEmpty && cachedChangesFile.get.head.canRead &&
      cachedComplementaryFile.nonEmpty && cachedComplementaryFile.get.nonEmpty && cachedComplementaryFile.get.head.canRead){
      Some(cachedGeometryFile.get.head, cachedChangesFile.get.head, cachedComplementaryFile.get.head)
    } else {
      None
    }
  }

  private def getNodeCacheFiles(municipalityCode: Int, dir: Option[File]): Option[File] = {
    val twentyHours = 20L * 60 * 60 * 1000

    deleteOldNodeCacheFiles(municipalityCode, dir, twentyHours)

    val cachedGeometryFile = dir.map(cacheDir => cacheDir.listFiles(new FilenameFilter {
      override def accept(dir: File, name: String): Boolean = {
        name.startsWith(nodeCacheStartsMatch.format(municipalityCode))
      }
    }).filter(f => f.lastModified() + twentyHours > System.currentTimeMillis))

    if (cachedGeometryFile.nonEmpty && cachedGeometryFile.get.nonEmpty && cachedGeometryFile.get.head.canRead) {
      Some(cachedGeometryFile.get.head)
    } else {
      None
    }
  }

  //getRoadLinksFromVVHFuture expects to get only "normal" roadlinks from getCachedRoadLinksAndChanges  method.
  private def getCachedRoadLinksAndChanges(municipalityCode: Int): (Seq[RoadLink], Seq[ChangeInfo]) = {
    val (roadLinks, changes, _) = getCachedRoadLinks(municipalityCode)
    (roadLinks, changes)
  }

  private def getCachedRoadLinksWithComplementaryAndChanges(municipalityCode: Int): (Seq[RoadLink], Seq[ChangeInfo]) = {
    val (roadLinks, changes, complementaries) = getCachedRoadLinks(municipalityCode)
    (roadLinks ++ complementaries, changes)
  }

  private def getCachedRoadLinks(municipalityCode: Int): (Seq[RoadLink], Seq[ChangeInfo], Seq[RoadLink]) = {
    val dir = getCacheDirectory
    val cachedFiles = getCacheWithComplementaryFiles(municipalityCode, dir)
    cachedFiles match {
      case Some((geometryFile, changesFile, complementaryFile)) =>
        logger.info("Returning cached result")
        (vvhSerializer.readCachedGeometry(geometryFile), vvhSerializer.readCachedChanges(changesFile), vvhSerializer.readCachedGeometry(complementaryFile))
      case _ =>
        val (roadLinks, changes, complementary) = reloadRoadLinksWithComplementaryAndChangesFromVVH(municipalityCode)
        if (dir.nonEmpty) {
          try {
            val newGeomFile = new File(dir.get, geometryCacheFileNames.format(municipalityCode, System.currentTimeMillis))
            if (vvhSerializer.writeCache(newGeomFile, roadLinks)) {
              logger.info("New cached file created: " + newGeomFile + " containing " + roadLinks.size + " items")
            } else {
              logger.error("Writing cached geom file failed!")
            }
            val newChangeFile = new File(dir.get, changeCacheFileNames.format(municipalityCode, System.currentTimeMillis))
            if (vvhSerializer.writeCache(newChangeFile, changes)) {
              logger.info("New cached file created: " + newChangeFile + " containing " + changes.size + " items")
            } else {
              logger.error("Writing cached changes file failed!")
            }
            val newComplementaryFile = new File(dir.get, complementaryCacheFileNames.format(municipalityCode, System.currentTimeMillis))
            if (vvhSerializer.writeCache(newComplementaryFile, complementary)) {
              logger.info("New cached file created: " + newComplementaryFile + " containing " + complementary.size + " items")
            } else {
              logger.error("Writing cached complementary file failed!")
            }
          } catch {
            case ex: Exception => logger.warn("Failed cache IO when writing:", ex)
          }
        }
        (roadLinks, changes, complementary)
    }
  }

  private def getCachedRoadNodes(municipalityCode: Int): Seq[VVHRoadNodes] = {
    val dir = getCacheDirectory
    val cachedFiles = getNodeCacheFiles(municipalityCode, dir)
    cachedFiles match {
      case Some(nodeFile) =>
        logger.info("Returning cached result")
        vvhSerializer.readCachedNodes(nodeFile)
      case _ =>
        val roadNodes = reloadRoadNodesFromVVH(municipalityCode)
        if (dir.nonEmpty) {
          try {
            val newGeomFile = new File(dir.get, nodeCacheFileNames.format(municipalityCode, System.currentTimeMillis))
            if (vvhSerializer.writeCache(newGeomFile, roadNodes)) {
              logger.info("New cached file created: " + newGeomFile + " containing " + roadNodes.size + " items")
            } else {
              logger.error("Writing cached geom file failed!")
            }
          } catch {
            case ex: Exception => logger.warn("Failed cache IO when writing:", ex)
          }
        }
        roadNodes
    }
  }

  def clearCache(): Int = {
    val dir = getCacheDirectory
    var cleared = 0
    dir.foreach(d => d.listFiles(new FilenameFilter {
      override def accept(dir: File, name: String): Boolean = {
        name.endsWith(allCacheEndsMatch)
      }
    }).foreach { f =>
      logger.info("Clearing cache: " + f.getAbsolutePath)
      f.delete()
      cleared = cleared + 1
    }
    )
    cleared
  }

  def getComplementaryRoadLinksFromVVH(bounds: BoundingRectangle, municipalities: Set[Int] = Set()): Seq[RoadLink] = {
    val vvhRoadLinks = Await.result(vvhClient.complementaryData.fetchByMunicipalitiesAndBoundsF(bounds, municipalities), atMost = Duration.create(1, TimeUnit.HOURS))
    withDynTransaction {
      (enrichRoadLinksFromVVH(vvhRoadLinks, Seq.empty[ChangeInfo]), Seq.empty[ChangeInfo])
    }._1
  }

  def getSuravageLinksFromVVHF(bounds: BoundingRectangle, municipalities: Set[Int] = Set()): Future[Seq[VVHRoadlink]] = {
    vvhClient.suravageData.fetchSuravageByMunicipalitiesAndBoundsF(bounds, municipalities)
  }

  def getSuravageRoadLinks(municipality: Int): Seq[RoadLink] = {
    val vvhRoadLinks = Await.result(vvhClient.suravageData.fetchSuravageByMunicipality(municipality), atMost = Duration.create(1, TimeUnit.HOURS))
    withDynTransaction {
      (enrichRoadLinksFromVVH(vvhRoadLinks, Seq.empty[ChangeInfo]), Seq.empty[ChangeInfo])
    }._1
  }

  def fetchSuravageLinksByLinkIdsFromVVH(linkIdsToGet: Set[Long]): Seq[VVHRoadlink] = {
    Await.result(vvhClient.suravageData.fetchSuravageByLinkIdsF(linkIdsToGet), atMost = Duration.create(1, TimeUnit.HOURS))
  }

  def getComplementaryRoadLinksFromVVH(municipality: Int): Seq[RoadLink] = {
    val vvhRoadLinks = Await.result(vvhClient.complementaryData.fetchByMunicipalityF(municipality), Duration.create(1, TimeUnit.HOURS))
    withDynTransaction {
      (enrichRoadLinksFromVVH(vvhRoadLinks, Seq.empty[ChangeInfo]), Seq.empty[ChangeInfo])
    }._1
  }

  def getViiteCurrentAndComplementaryRoadLinksFromVVH(municipality: Int, roadNumbers: Seq[(Int, Int)], frozenTimeVVHAPIServiceEnabled: Boolean = false): Seq[RoadLink] = {
    val complementaryF = vvhClient.complementaryData.fetchByMunicipalityAndRoadNumbersF(municipality, roadNumbers)
    val currentF = if (frozenTimeVVHAPIServiceEnabled) vvhClient.frozenTimeRoadLinkData.fetchByMunicipalityAndRoadNumbersF(municipality, roadNumbers) else vvhClient.roadLinkData.fetchByMunicipalityAndRoadNumbersF(municipality, roadNumbers)
    val (compLinks, vvhRoadLinks) = Await.result(complementaryF.zip(currentF), atMost = Duration.create(1, TimeUnit.HOURS))
    (enrichRoadLinksFromVVH(compLinks ++ vvhRoadLinks, Seq.empty[ChangeInfo]), Seq.empty[ChangeInfo])._1
  }

  def getCurrentAndComplementaryVVHRoadLinks(linkIds: Set[Long], frozenTimeVVHAPIServiceEnabled : Boolean = false): Seq[VVHRoadlink] = {
    val roadLinks= if(frozenTimeVVHAPIServiceEnabled) vvhClient.frozenTimeRoadLinkData.fetchByLinkIds(linkIds) else vvhClient.roadLinkData.fetchByLinkIds(linkIds)
    vvhClient.complementaryData.fetchByLinkIds(linkIds) ++ roadLinks
  }

  def getCurrentAndComplementaryAndSuravageRoadLinksFromVVH(linkIds: Set[Long], newTransaction: Boolean = true, frozenTimeVVHAPIServiceEnabled : Boolean = false ): Seq[RoadLink] = {
    val roadLinks = if (frozenTimeVVHAPIServiceEnabled) vvhClient.frozenTimeRoadLinkData.fetchByLinkIds(linkIds) else vvhClient.roadLinkData.fetchByLinkIds(linkIds)
    val roadLinksSuravage = vvhClient.suravageData.fetchSuravageByLinkIds(linkIds)
    val roadLinksVVH = vvhClient.complementaryData.fetchByLinkIds(linkIds) ++ roadLinks ++ roadLinksSuravage
    if (newTransaction)
      withDynTransaction {
        enrichRoadLinksFromVVH(roadLinksVVH)
      }
    else
      enrichRoadLinksFromVVH(roadLinksVVH)
  }

  case class RoadLinkRoadAddress(linkId: Long, roadNumber: Long, roadPartNumber: Long, track: Track, sideCode: SideCode,
                                 startAddrMValue: Long, endAddrMValue: Long) {
    def asAttributes: Map[String, Any] = Map("VIITE_ROAD_NUMBER" -> roadNumber,
      "VIITE_ROAD_PART_NUMBER" -> roadPartNumber,
      "VIITE_TRACK" -> track.value,
      "VIITE_SIDECODE" -> sideCode.value,
      "VIITE_START_ADDR" -> startAddrMValue,
      "VIITE_END_ADDR" -> endAddrMValue)
  }

  /**
    * This method returns Road Link that have been changed in VVH between given dates values. It is used by TN-ITS ChangeApi.
    *
    * @param sinceDate
    * @param untilDate
    * @return Changed Road Links between given dates
    */
  def getChanged(sinceDate: DateTime, untilDate: DateTime): Seq[ChangedVVHRoadlink] = {
    val municipalitiesCodeToValidate = List(35, 43, 60, 62, 65, 76, 170, 295, 318, 417, 438, 478, 736, 766, 771, 941)
    val timezone = DateTimeZone.forOffsetHours(0)

    val roadLinks =
      withDynTransaction {
        enrichRoadLinksFromVVH(fetchChangedVVHRoadlinksBetweenDates(sinceDate, untilDate))
      }

    roadLinks.map { roadLink =>
      ChangedVVHRoadlink(
        link = roadLink,
        value =
          if (municipalitiesCodeToValidate.contains(roadLink.municipalityCode)) {
            roadLink.attributes.getOrElse("ROADNAME_SE", "").toString
          } else {
            roadLink.attributes.getOrElse("ROADNAME_FI", "").toString
          },
        createdAt = roadLink.attributes.get("CREATED_DATE") match {
          case Some(date) => Some(new DateTime(date.asInstanceOf[BigInt].toLong, timezone))
          case _ => None
        },
        changeType = "Modify"
      )
    }
  }

}
