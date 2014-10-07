package fi.liikennevirasto.digiroad2.linearasset.oracle

import fi.liikennevirasto.digiroad2.Point
import fi.liikennevirasto.digiroad2.asset.BoundingRectangle
import fi.liikennevirasto.digiroad2.oracle.OracleDatabase._
import fi.liikennevirasto.digiroad2.asset.oracle.Queries._
import fi.liikennevirasto.digiroad2.util.{SpeedLimitLinkPositions, GeometryUtils}
import oracle.jdbc.OracleConnection

import org.scalatest.FunSuite
import org.scalatest.Matchers
import org.scalatest.Tag

import oracle.spatial.geometry.JGeometry
import scala.slick.jdbc.{StaticQuery => Q}
import Q.interpolation
import scala.slick.driver.JdbcDriver.backend.Database
import Database.dynamicSession

class OracleLinearAssetDaoSpec extends FunSuite with Matchers {

  private def truncateLinkGeometry(linkId: Long, startMeasure: Double, endMeasure: Double): Seq[(Double, Double)] = {
    val truncatedGeometry: Array[Byte] = sql"""
      select to_2d(sdo_lrs.dynamic_segment(rl.geom, $startMeasure, $endMeasure))
        from ROAD_LINK rl
        where id = $linkId
        """.as[Array[Byte]].list.head
    val points = JGeometry.load(truncatedGeometry).getOrdinatesArray.grouped(2)
    points.map { pointArray =>
      (pointArray(0), pointArray(1))
    }.toSeq
  }

  def assertSpeedLimitEndPointsOnLink(speedLimitId: Long, roadLinkId: Long, startMeasure: Double, endMeasure: Double) = {
    val expectedEndPoints = GeometryUtils.geometryEndpoints(truncateLinkGeometry(roadLinkId, startMeasure, endMeasure).toList)
    val limitEndPoints = GeometryUtils.geometryEndpoints(OracleLinearAssetDao.getSpeedLimitLinksWithLength(speedLimitId).find { link => link._1 == roadLinkId }.get._3)
    expectedEndPoints._1.distanceTo(limitEndPoints._1) should be(0.0 +- 0.01)
    expectedEndPoints._2.distanceTo(limitEndPoints._2) should be(0.0 +- 0.01)
  }

  test("splitting one link speed limit " +
    "where split measure is after link middle point " +
    "modifies end measure of existing speed limit " +
    "and creates new speed limit for second split", Tag("db")) {
    Database.forDataSource(ds).withDynTransaction {
      val createdId = OracleLinearAssetDao.splitSpeedLimit(700114, 5537, 100, "test")
      val (existingModifiedBy, _, _, _, _, _) = OracleLinearAssetDao.getSpeedLimitDetails(700114)
      val (_, _, newCreatedBy, _, _, _) = OracleLinearAssetDao.getSpeedLimitDetails(createdId)

      assertSpeedLimitEndPointsOnLink(700114, 5537, 0, 100)
      assertSpeedLimitEndPointsOnLink(createdId, 5537, 100, 136.788)

      existingModifiedBy shouldBe Some("test")
      newCreatedBy shouldBe Some("test")
      dynamicSession.rollback()
    }
  }

  test("splitting one link speed limit " +
    "where split measure is before link middle point " +
    "modifies start measure of existing speed limit " +
    "and creates new speed limit for first split", Tag("db")) {
    Database.forDataSource(ds).withDynTransaction {
      val createdId = OracleLinearAssetDao.splitSpeedLimit(700114, 5537, 50, "test")
      val (modifiedBy, _, _, _, _, _) = OracleLinearAssetDao.getSpeedLimitDetails(700114)
      val (_, _, newCreatedBy, _, _, _) = OracleLinearAssetDao.getSpeedLimitDetails(createdId)

      assertSpeedLimitEndPointsOnLink(700114, 5537, 50, 136.788)
      assertSpeedLimitEndPointsOnLink(createdId, 5537, 0, 50)

      modifiedBy shouldBe Some("test")
      newCreatedBy shouldBe Some("test")
      dynamicSession.rollback()
    }
  }

  test("splitting three link speed limit " +
    "where first split is longer than second" +
    "existing speed limit should cover only first split", Tag("db")) {
    Database.forDataSource(ds).withDynTransaction {
      OracleLinearAssetDao.splitSpeedLimit(700490, 5695, 150, "test")
      val existingLinks = OracleLinearAssetDao.getSpeedLimitLinksWithLength(700490)

      existingLinks.length shouldBe 2
      existingLinks.map(_._1) should contain only (5752, 5695)
      dynamicSession.rollback()
    }
  }

  test("splitting three link speed limit " +
    "where first split is shorter than second" +
    "existing speed limit should cover only second split", Tag("db")) {
    Database.forDataSource(ds).withDynTransaction {
      OracleLinearAssetDao.splitSpeedLimit(700490, 5695, 10, "test")
      val existingLinks = OracleLinearAssetDao.getSpeedLimitLinksWithLength(700490)

      existingLinks.length shouldBe 2
      existingLinks.map(_._1) should contain only (5695, 5904)
      dynamicSession.rollback()
    }
  }

  test("splitting speed limit " +
    "so that shorter split contains multiple linear references " +
    "moves all linear references to newly created speed limit", Tag("db")) {
    Database.forDataSource(ds).withDynTransaction {
      val createdId = OracleLinearAssetDao.splitSpeedLimit(700642, 5872, 148, "test")
      val createdLinks = OracleLinearAssetDao.getSpeedLimitLinksWithLength(createdId)

      createdLinks.length shouldBe 3
      createdLinks.map(_._1) should contain only (5613, 5631, 5872)
      dynamicSession.rollback()
    }
  }

  test("using road link service returns identical results to direct database query", Tag("db")) {
   Database.forDataSource(ds).withDynTransaction {
     val boundingBox = BoundingRectangle(Point(371560, 6674372), Point(375344, 6679784))

     val timeBeforeDbFetch = System.currentTimeMillis()
     val dbLinks = OracleLinearAssetDao.getSpeedLimitLinksByBoundingBox(boundingBox)
     val timeAfterDbFetch = System.currentTimeMillis()

     val timeBeforeServiceFetch = System.currentTimeMillis()
     val serviceLinks = OracleLinearAssetDao.getSpeedLimitLinksByBoundingBox2(boundingBox)
     val timeAfterServiceFetch = System.currentTimeMillis()

     println("*** Calculated time for DB fetch: " + (timeAfterDbFetch - timeBeforeDbFetch))
     println("*** Calculated time for service fetch: " + (timeAfterServiceFetch - timeBeforeServiceFetch))
     serviceLinks should contain theSameElementsAs dbLinks
   }
  }

  test("truncate empty geometry") {
    val truncated = OracleLinearAssetDao.truncateGeometry(Nil, 10, 15)
    truncated should be (Nil)
  }

  test("truncation fails when start measure is after end measure") {
    an [IllegalArgumentException] should be thrownBy OracleLinearAssetDao.truncateGeometry(Nil, 15, 10)
  }

  test("truncation fails on one point geometry") {
    an [IllegalArgumentException] should be thrownBy OracleLinearAssetDao.truncateGeometry(Seq(Point(0.0, 0.0)), 10, 15)
  }
}
