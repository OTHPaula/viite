package fi.liikennevirasto.viite.dao

import fi.liikennevirasto.digiroad2.{DigiroadEventBus, Point}
import fi.liikennevirasto.digiroad2.dao.Sequences
import fi.liikennevirasto.digiroad2.oracle.OracleDatabase
import fi.liikennevirasto.digiroad2.service.RoadLinkService
import fi.liikennevirasto.viite.process.RoadwayAddressMapper
import org.joda.time.DateTime
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FunSuite, Matchers}
import slick.driver.JdbcDriver.backend.Database
import slick.driver.JdbcDriver.backend.Database.dynamicSession

/**
  * Class to test DB trigger that does not allow reserving already reserved parts to project
  */
class ProjectReservedPartDAOSpec extends FunSuite with Matchers {
  val mockRoadLinkService = MockitoSugar.mock[RoadLinkService]
  val mockEventBus = MockitoSugar.mock[DigiroadEventBus]
  val mockRoadwayAddressMapper = MockitoSugar.mock[RoadwayAddressMapper]
  val mockLinearLocationDAO = MockitoSugar.mock[LinearLocationDAO]
  val mockRoadwayDAO = MockitoSugar.mock[RoadwayDAO]
  val mockRoadNetworkDAO = MockitoSugar.mock[RoadNetworkDAO]

  def runWithRollback(f: => Unit): Unit = {
    // Prevent deadlocks in DB because we create and delete links in tests and don't handle the project ids properly
    // TODO: create projects with unique ids so we don't get into transaction deadlocks in tests
    Database.forDataSource(OracleDatabase.ds).withDynTransaction {
      f
      dynamicSession.rollback()
    }
  }
  val roadwayDAO = new RoadwayDAO
  val projectDAO = new ProjectDAO
  val projectLinkDAO = new ProjectLinkDAO
  val projectReservedPartDAO = new ProjectReservedPartDAO

  private def dummyRoadAddressProject(id: Long, status: ProjectState, reservedParts: Seq[ProjectReservedPart] = List.empty[ProjectReservedPart], ely: Option[Long] = None, coordinates: Option[ProjectCoordinates] = None): RoadAddressProject ={
    RoadAddressProject(id, status, "testProject", "testUser", DateTime.parse("1901-01-01"), "testUser", DateTime.parse("1901-01-01"), DateTime.now(), "additional info here", reservedParts, Some("status info"), ely, coordinates)
  }

  test("Test reserveRoadPart When having reserved one project with that part Then should fetch it without any problems") {
    runWithRollback {
      val reservedParts = Seq(ProjectReservedPart(5: Long, 203: Long, 203: Long, Some(6L), Some(Discontinuity.apply("jatkuva")), Some(8L), newLength = None, newDiscontinuity = None, newEly = None))
      val id = Sequences.nextViitePrimaryKeySeqValue
      val rap = dummyRoadAddressProject(id, ProjectState.Incomplete, reservedParts, Some(8L), None)
      projectDAO.createRoadAddressProject(rap)
      projectReservedPartDAO.reserveRoadPart(id, 5, 203, "TestUser")
      val fetchedPart = projectReservedPartDAO.fetchReservedRoadPart(5, 203)
      fetchedPart.nonEmpty should be (true)
      fetchedPart.get.id should be (id)
      fetchedPart.get.roadNumber should be (reservedParts.head.roadNumber)
      fetchedPart.get.roadPartNumber should be (reservedParts.head.roadPartNumber)
    }
  }

  //TODO will be implement at VIITE-1539
  //  test("roadpart reserved, fetched with and without filtering, and released by project test") {
  //    //Creation of Test road
  //    runWithRollback {
  //      val id = Sequences.nextViitePrimaryKeySeqValue
  //      val rap = RoadAddressProject(id, ProjectState.apply(1), "TestProject", "TestUser", DateTime.parse("1901-01-01"), "TestUser", DateTime.parse("1901-01-01"), DateTime.now(), "Some additional info", List.empty, None)
  //      ProjectDAO.createRoadAddressProject(rap)
  //      ProjectDAO.reserveRoadPart(id, 5, 203, rap.createdBy)
  //      val addresses = RoadAddressDAO.fetchByRoadPart(5, 203).map(toProjectLink(rap))
  //      ProjectDAO.create(addresses)
  //      val project = ProjectDAO.roadPartReservedByProject(5, 203)
  //      project should be(Some("TestProject"))
  //      val reserved = ProjectDAO.fetchReservedRoadPart(5, 203)
  //      reserved.nonEmpty should be(true)
  //      ProjectDAO.fetchReservedRoadParts(id) should have size (1)
  //      ProjectDAO.removeReservedRoadPart(id, reserved.get)
  //      val projectAfter = ProjectDAO.roadPartReservedByProject(5, 203)
  //      projectAfter should be(None)
  //      ProjectDAO.fetchReservedRoadPart(5, 203).isEmpty should be(true)
  //    }
  //  }

  //TODO will be implement at VIITE-1539
  //  test("fetch by road parts") {
  //    //Creation of Test road
  //    runWithRollback {
  //      val id = Sequences.nextViitePrimaryKeySeqValue
  //      val rap = RoadAddressProject(id, ProjectState.apply(1), "TestProject", "TestUser", DateTime.parse("1901-01-01"), "TestUser", DateTime.parse("1901-01-01"), DateTime.now(), "Some additional info", List.empty, None)
  //      ProjectDAO.createRoadAddressProject(rap)
  //      ProjectDAO.reserveRoadPart(id, 5, 203, rap.createdBy)
  //      ProjectDAO.reserveRoadPart(id, 5, 205, rap.createdBy)
  //      val addresses = (RoadAddressDAO.fetchByRoadPart(5, 203) ++ RoadAddressDAO.fetchByRoadPart(5, 205)).map(toProjectLink(rap))
  //      ProjectDAO.create(addresses)
  //      ProjectDAO.roadPartReservedByProject(5, 203) should be(Some("TestProject"))
  //      ProjectDAO.roadPartReservedByProject(5, 205) should be(Some("TestProject"))
  //      val reserved203 = ProjectDAO.fetchByProjectRoadParts(Set((5L, 203L)), id)
  //      reserved203.nonEmpty should be (true)
  //      val reserved205 = ProjectDAO.fetchByProjectRoadParts(Set((5L, 205L)), id)
  //      reserved205.nonEmpty should be (true)
  //      reserved203 shouldNot be (reserved205)
  //      reserved203.toSet.intersect(reserved205.toSet) should have size (0)
  //      val reserved = ProjectDAO.fetchByProjectRoadParts(Set((5L,203L), (5L, 205L)), id)
  //      reserved.map(_.id).toSet should be (reserved203.map(_.id).toSet ++ reserved205.map(_.id).toSet)
  //      reserved should have size (addresses.size)
  //    }
  //  }

}