package fi.liikennevirasto.viite.process

import java.util.Properties

import fi.liikennevirasto.digiroad2.asset.ConstructionType.InUse
import fi.liikennevirasto.digiroad2.asset._
import fi.liikennevirasto.digiroad2.{DigiroadEventBus, GeometryUtils, Point}
import fi.liikennevirasto.digiroad2.asset.LinkGeomSource.NormalLinkInterface
import fi.liikennevirasto.digiroad2.asset.SideCode.{AgainstDigitizing, TowardsDigitizing}
import fi.liikennevirasto.digiroad2.dao.{Queries, Sequences}
import fi.liikennevirasto.digiroad2.linearasset.{PolyLine, RoadLink}
import fi.liikennevirasto.digiroad2.oracle.OracleDatabase
import fi.liikennevirasto.digiroad2.service.{RoadLinkService, RoadLinkType}
import fi.liikennevirasto.digiroad2.util.Track
import fi.liikennevirasto.viite._
import fi.liikennevirasto.viite.RoadType.PublicRoad
import fi.liikennevirasto.viite.dao.Discontinuity.Continuous
import fi.liikennevirasto.viite.dao._
import fi.liikennevirasto.viite.dao.TerminationCode.NoTermination
import fi.liikennevirasto.viite.model.{Anomaly, ProjectAddressLink, RoadAddressLinkLike}
import fi.liikennevirasto.viite.util._
import org.joda.time.DateTime
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
import slick.driver.JdbcDriver.backend.Database
import slick.driver.JdbcDriver.backend.Database.dynamicSession
import slick.jdbc.StaticQuery.interpolation
import slick.jdbc.{StaticQuery => Q}

import scala.util.parsing.json.JSON

/**
  * Created by marquesrf on 08-03-2018.
  */
class CommonHistoryFillerSpec extends FunSuite with Matchers with BeforeAndAfter {
  def withDynTransaction[T](f: => T): T = OracleDatabase.withDynTransaction(f)

  def runWithRollback[T](f: => T): T = {
    Database.forDataSource(OracleDatabase.ds).withDynTransaction {
      val t = f
      dynamicSession.rollback()
      t
    }
  }
  val properties: Properties = {
    val props = new Properties()
    props.load(getClass.getResourceAsStream("/digiroad2.properties"))
    props
  }
  val mockProjectService = MockitoSugar.mock[ProjectService]
  val mockRoadLinkService = MockitoSugar.mock[RoadLinkService]
  val mockRoadAddressService = MockitoSugar.mock[RoadAddressService]
  val mockEventBus = MockitoSugar.mock[DigiroadEventBus]
  val roadAddressService = new RoadAddressService(mockRoadLinkService, mockEventBus) {
    override def withDynSession[T](f: => T): T = f

    override def withDynTransaction[T](f: => T): T = f
  }

  val projectService = new ProjectService(roadAddressService, mockRoadLinkService, mockEventBus) {
    override def withDynSession[T](f: => T): T = f

    override def withDynTransaction[T](f: => T): T = f
  }

  private def toMockAnswer(projectLinks: Seq[ProjectLink], roadLink: RoadLink, seq: Seq[RoadLink] = Seq()) = {
    new Answer[Seq[RoadLink]]() {
      override def answer(invocation: InvocationOnMock): Seq[RoadLink] = {
        val ids = if (invocation.getArguments.apply(0) == null)
          Set[Long]()
        else invocation.getArguments.apply(0).asInstanceOf[Set[Long]]
        projectLinks.groupBy(_.linkId).filterKeys(l => ids.contains(l)).mapValues { pl =>
          val startP = Point(pl.map(_.startAddrMValue).min, 0.0)
          val endP = Point(pl.map(_.endAddrMValue).max, 0.0)
          val maxLen = pl.map(_.endMValue).max
          val midP = Point((startP.x + endP.x) * .5,
            if (endP.x - startP.x < maxLen) {
              Math.sqrt(maxLen * maxLen - (startP.x - endP.x) * (startP.x - endP.x)) / 2
            }
            else 0.0)
          val forcedGeom = pl.filter(l => l.id == -1000L && l.geometry.nonEmpty).sortBy(_.startAddrMValue)
          val (startFG, endFG) = (forcedGeom.headOption.map(_.startingPoint), forcedGeom.lastOption.map(_.endPoint))
          if (pl.head.id == -1000L) {
            roadLink.copy(linkId = pl.head.linkId, geometry = Seq(startFG.get, endFG.get))
          } else
            roadLink.copy(linkId = pl.head.linkId, geometry = Seq(startP, midP, endP))
        }.values.toSeq ++ seq
      }
    }
  }

  private def mockForProject[T <: PolyLine](id: Long, l: Seq[T] = Seq()) = {
    val roadLink = RoadLink(Sequences.nextViitePrimaryKeySeqValue, Seq(Point(535602.222, 6982200.25, 89.9999), Point(535605.272, 6982204.22, 85.90899999999965))
      , 540.3960283713503, State, 99, TrafficDirection.AgainstDigitizing, UnknownLinkType, Some("25.06.2015 03:00:00"), Some("vvh_modified"), Map("MUNICIPALITYCODE" -> BigInt.apply(749)),
      InUse, NormalLinkInterface)
    val (projectLinks, palinks) = l.partition(_.isInstanceOf[ProjectLink])
    val dbLinks = ProjectDAO.getProjectLinks(id)
    when(mockRoadLinkService.getViiteRoadLinksHistoryFromVVH(any[Set[Long]])).thenReturn(Seq())
    when(mockRoadLinkService.getViiteRoadLinksByLinkIdsFromVVH(any[Set[Long]], any[Boolean], any[Boolean])).thenAnswer(
      toMockAnswer(dbLinks ++ projectLinks.asInstanceOf[Seq[ProjectLink]].filterNot(l => dbLinks.map(_.linkId).contains(l.linkId)),
        roadLink, palinks.asInstanceOf[Seq[ProjectAddressLink]].map(toRoadLink)
      ))
  }

  private def toRoadLink(ral: RoadAddressLinkLike): RoadLink = {
    RoadLink(ral.linkId, ral.geometry, ral.length, ral.administrativeClass, 1,
      extractTrafficDirection(ral.sideCode, Track.apply(ral.trackCode.toInt)), ral.linkType, ral.modifiedAt, ral.modifiedBy, Map(
        "MUNICIPALITYCODE" -> BigInt(749), "VERTICALLEVEL" -> BigInt(1), "SURFACETYPE" -> BigInt(1),
        "ROADNUMBER" -> BigInt(ral.roadNumber), "ROADPARTNUMBER" -> BigInt(ral.roadPartNumber)),
      ral.constructionType, ral.roadLinkSource)
  }

  private def extractTrafficDirection(sideCode: SideCode, track: Track): TrafficDirection = {
    (sideCode, track) match {
      case (_, Track.Combined) => TrafficDirection.BothDirections
      case (TowardsDigitizing, Track.RightSide) => TrafficDirection.TowardsDigitizing
      case (TowardsDigitizing, Track.LeftSide) => TrafficDirection.AgainstDigitizing
      case (AgainstDigitizing, Track.RightSide) => TrafficDirection.AgainstDigitizing
      case (AgainstDigitizing, Track.LeftSide) => TrafficDirection.TowardsDigitizing
      case (_, _) => TrafficDirection.UnknownDirection
    }
  }

  def toProjectLink(project: RoadAddressProject)(roadAddress: RoadAddress): ProjectLink = {
    ProjectLink(roadAddress.id, roadAddress.roadNumber, roadAddress.roadPartNumber, roadAddress.track,
      roadAddress.discontinuity, roadAddress.startAddrMValue, roadAddress.endAddrMValue, roadAddress.startDate,
      roadAddress.endDate, modifiedBy=Option(project.createdBy), 0L, roadAddress.linkId, roadAddress.startMValue, roadAddress.endMValue,
      roadAddress.sideCode, roadAddress.calibrationPoints, floating=false, roadAddress.geometry, project.id, LinkStatus.NotHandled, RoadType.PublicRoad,
      roadAddress.linkGeomSource, GeometryUtils.geometryLength(roadAddress.geometry), roadAddress.id, roadAddress.ely,false,
      None, roadAddress.adjustedTimestamp)
  }

  test("Unchanged addresses with new Road Type between different commonHistoryId groups") {
    var count = 0
    val roadLinks = Seq(
      RoadLink(5170939L, Seq(Point(535605.272, 6982204.22, 85.90899999999965))
        , 540.3960283713503, State, 99, TrafficDirection.AgainstDigitizing, UnknownLinkType, Some("25.06.2015 03:00:00"), Some("vvh_modified"), Map("MUNICIPALITYCODE" -> BigInt.apply(749)),
        InUse, NormalLinkInterface))
    runWithRollback {
      val countCurrentProjects = projectService.getRoadAddressAllProjects()
      val id = 0
      val addresses = List(
        ReservedRoadPart(Sequences.nextViitePrimaryKeySeqValue: Long, 5: Long, 207: Long, Some(5L), Some(Discontinuity.apply("jatkuva")), Some(8L), newLength = None, newDiscontinuity = None, newEly = None))
      val roadAddressProject = RoadAddressProject(id, ProjectState.apply(1), "TestProject", "TestUser", DateTime.now(),
        "TestUser", DateTime.parse("1901-01-01"), DateTime.now(), "Some additional info", Seq(), None)
      val saved = projectService.createRoadLinkProject(roadAddressProject)
      mockForProject(saved.id, RoadAddressDAO.fetchByRoadPart(5, 207).map(toProjectLink(saved)))
      projectService.saveProject(saved.copy(reservedParts = addresses))
      val countAfterInsertProjects = projectService.getRoadAddressAllProjects()
      count = countCurrentProjects.size + 1
      countAfterInsertProjects.size should be(count)
      val projectLinks = ProjectDAO.getProjectLinks(saved.id)
      projectLinks.isEmpty should be(false)
      val partitioned = projectLinks.partition(_.roadPartNumber == 207)
      val linkIds207 = partitioned._1.map(_.linkId).toSet
      when(mockRoadLinkService.getViiteRoadLinksByLinkIdsFromVVH(linkIds207, false, false)).thenReturn(
        partitioned._1.map(pl => roadLinks.head.copy(linkId = pl.linkId, geometry = Seq(Point(pl.startAddrMValue, 0.0), Point(pl.endAddrMValue, 0.0)))))

      val addressIds207 = partitioned._1.map(_.roadAddressId).toSet
      val filter = s" (${addressIds207.mkString(",")}) "
      sqlu""" update project_link set status=1 WHERE road_address_id in #$filter""".execute
      val linksAfterUpdate = ProjectDAO.getProjectLinks(saved.id)
      linksAfterUpdate.foreach(l=> l.status should be (LinkStatus.UnChanged))
      //second and last but one from linkIds
      val ids = Seq(projectLinks(1),projectLinks(projectLinks.size - 2)).map(_.roadAddressId).toSet
      val filter2 = s" (${ids.mkString(",")}) "
      sqlu""" update project_link set road_type=3 WHERE road_address_id in #$filter2""".execute

      val nextCommonId = Sequences.nextCommonHistorySeqValue
      sqlu""" update road_address set common_history_id= $nextCommonId WHERE road_number = 5 and road_part_number = 207 and end_date is null and start_addr_m > 1000""".execute
      val afterAddressUpdates = RoadAddressDAO.fetchByRoadPart(addresses.head.roadNumber, addresses.head.roadPartNumber)
      afterAddressUpdates.groupBy(_.commonHistoryId).size should be (2)
      sqlu""" update project set state=5, tr_id = 1 WHERE id=${saved.id}""".execute
      ProjectDAO.getProjectStatus(saved.id) should be(Some(ProjectState.Saved2TR))
      projectService.updateRoadAddressWithProjectLinks(ProjectState.Saved2TR, saved.id)
      ProjectDAO.getProjectLinks(saved.id).size should be (0)

      val roadAddresses = RoadAddressDAO.fetchByRoadPart(addresses.head.roadNumber, addresses.head.roadPartNumber)
      roadAddresses.groupBy(_.commonHistoryId).size should be (6)
    }
  }

  test("New addresses with new Road Type between") {
    runWithRollback {
      sqlu"DELETE FROM ROAD_ADDRESS WHERE ROAD_NUMBER=75 AND ROAD_PART_NUMBER=2".execute
      val id = Sequences.nextViitePrimaryKeySeqValue
      val rap = RoadAddressProject(id, ProjectState.apply(1), "TestProject", "TestUser", DateTime.parse("1901-01-01"), "TestUser", DateTime.parse("1901-01-01"), DateTime.now(), "Some additional info", List.empty, None)
      ProjectDAO.createRoadAddressProject(rap)

      val geometries = StaticTestData.mappedGeoms(Set(5176552, 5176512, 5176584, 5502405, 5502441, 5502444))
      val geom5176552 = geometries(5176552)
      val geom5176512 = geometries(5176512)
      val geom5176584 = geometries(5176584)
      val geom5502405 = geometries(5502405)
      val geom5502441 = geometries(5502441)
      val geom5502444 = geometries(5502444)

      val addProjectAddressLink5176552 = ProjectAddressLink(NewRoadAddress, 5176552, geom5176552, GeometryUtils.geometryLength(geom5176552),
        State, Motorway, RoadLinkType.NormalRoadLinkType, ConstructionType.InUse, LinkGeomSource.NormalLinkInterface,
        RoadType.PublicRoad, "X", 749, None, None, Map.empty, 75, 2, 0L, 8L, 5L, 0L, 0L, 0.0, GeometryUtils.geometryLength(geom5176552),
        SideCode.TowardsDigitizing, None, None, Anomaly.None, 0L, LinkStatus.New, 0)
      val addProjectAddressLink5176512 = ProjectAddressLink(NewRoadAddress, 5176512, geom5176512, GeometryUtils.geometryLength(geom5176512),
        State, Motorway, RoadLinkType.NormalRoadLinkType, ConstructionType.InUse, LinkGeomSource.NormalLinkInterface,
        RoadType.PublicRoad, "X", 749, None, None, Map.empty, 75, 2, 0L, 8L, 5L, 0L, 0L, 0.0, GeometryUtils.geometryLength(geom5176512),
        SideCode.TowardsDigitizing, None, None, Anomaly.None, 0L, LinkStatus.New, 0)
      val addProjectAddressLink5176584 = ProjectAddressLink(NewRoadAddress, 5176584, geom5176584, GeometryUtils.geometryLength(geom5176584),
        State, Motorway, RoadLinkType.NormalRoadLinkType, ConstructionType.InUse, LinkGeomSource.NormalLinkInterface,
        RoadType.PublicRoad, "X", 749, None, None, Map.empty, 75, 2, 0L, 8L, 5L, 0L, 0L, 0.0, GeometryUtils.geometryLength(geom5176584),
        SideCode.TowardsDigitizing, None, None, Anomaly.None, 0L, LinkStatus.New, 0)
      val addProjectAddressLink5502405 = ProjectAddressLink(NewRoadAddress, 5176512, geom5502405, GeometryUtils.geometryLength(geom5502405),
        State, Motorway, RoadLinkType.NormalRoadLinkType, ConstructionType.InUse, LinkGeomSource.NormalLinkInterface,
        RoadType.PublicRoad, "X", 749, None, None, Map.empty, 75, 2, 0L, 8L, 5L, 0L, 0L, 0.0, GeometryUtils.geometryLength(geom5502405),
        SideCode.TowardsDigitizing, None, None, Anomaly.None, 0L, LinkStatus.New, 0)
      val addProjectAddressLink5502441 = ProjectAddressLink(NewRoadAddress, 5176512, geom5502441, GeometryUtils.geometryLength(geom5502441),
        State, Motorway, RoadLinkType.NormalRoadLinkType, ConstructionType.InUse, LinkGeomSource.NormalLinkInterface,
        RoadType.PublicRoad, "X", 749, None, None, Map.empty, 75, 2, 0L, 8L, 5L, 0L, 0L, 0.0, GeometryUtils.geometryLength(geom5502441),
        SideCode.TowardsDigitizing, None, None, Anomaly.None, 0L, LinkStatus.New, 0)
      val addProjectAddressLink5502444 = ProjectAddressLink(NewRoadAddress, 5176512, geom5502444, GeometryUtils.geometryLength(geom5502444),
        State, Motorway, RoadLinkType.NormalRoadLinkType, ConstructionType.InUse, LinkGeomSource.NormalLinkInterface,
        RoadType.PublicRoad, "X", 749, None, None, Map.empty, 75, 2, 0L, 8L, 5L, 0L, 0L, 0.0, GeometryUtils.geometryLength(geom5502444),
        SideCode.TowardsDigitizing, None, None, Anomaly.None, 0L, LinkStatus.New, 0)
      val addresses = Seq(addProjectAddressLink5176552, addProjectAddressLink5176512, addProjectAddressLink5176584, addProjectAddressLink5502405, addProjectAddressLink5502441, addProjectAddressLink5502444)
      mockForProject(id, addresses)
      projectService.addNewLinksToProject(addresses.map(backToProjectLink(rap)).map(_.copy(status = LinkStatus.New)), id, "U", addresses.minBy(_.endMValue).linkId) should be(None)
      val links = ProjectDAO.getProjectLinks(id)

      val ids = Seq(links(1),links(links.size - 2)).map(_.id).toSet
      val filter2 = s" (${ids.mkString(",")}) "
      sqlu""" update project_link set road_type=3 WHERE id in #$filter2""".execute

      val linksAfterUpdate = ProjectDAO.getProjectLinks(id)
      sqlu""" update project set state=5, tr_id = 1 WHERE id=${id}""".execute
      ProjectDAO.getProjectStatus(id) should be(Some(ProjectState.Saved2TR))
      projectService.updateRoadAddressWithProjectLinks(ProjectState.Saved2TR, rap.id)
      ProjectDAO.getProjectLinks(id).size should be (0)

      val roadAddresses = RoadAddressDAO.fetchByRoadPart(addresses.head.roadNumber, addresses.head.roadPartNumber)
      roadAddresses.groupBy(_.commonHistoryId).size should be (5)
    }
  }
}
