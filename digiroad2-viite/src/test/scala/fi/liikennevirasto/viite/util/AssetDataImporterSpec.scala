package fi.liikennevirasto.viite.util

import fi.liikennevirasto.digiroad2.{GeometryUtils, Point}
import fi.liikennevirasto.digiroad2.asset.SideCode.Unknown
import fi.liikennevirasto.digiroad2.asset._
import fi.liikennevirasto.digiroad2.client.vvh._
import fi.liikennevirasto.digiroad2.util.{TestTransactions, Track}
import fi.liikennevirasto.viite.dao.{CalibrationPoint, RoadAddress, RoadAddressDAO}
import fi.liikennevirasto.digiroad2.linearasset.RoadLink
import org.joda.time.format.DateTimeFormat
import org.mockito.Matchers.any
import org.mockito.Mockito.when
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FunSuite, Matchers}
import slick.driver.JdbcDriver.backend.Database
import Database.dynamicSession
import fi.liikennevirasto.digiroad2.asset.ConstructionType.InUse
import fi.liikennevirasto.digiroad2.asset.LinkGeomSource.NormalLinkInterface
import fi.liikennevirasto.digiroad2.dao.RoadAddressDAO
import fi.liikennevirasto.digiroad2.oracle.OracleDatabase
import fi.liikennevirasto.digiroad2.service.RoadLinkService
import fi.liikennevirasto.viite.RoadType
import fi.liikennevirasto.viite.dao.Discontinuity.Discontinuous
import fi.liikennevirasto.viite.dao.TerminationCode.NoTermination
import org.joda.time.DateTime
import slick.driver.JdbcDriver.backend.DatabaseDef
import slick.jdbc.StaticQuery.{interpolation, _}
import slick.jdbc.{StaticQuery => Q}

class AssetDataImporterSpec extends FunSuite with Matchers {
  private val assetDataImporter = new AssetDataImporter {
    override def withDynTransaction(f: => Unit): Unit = f

    override def withDynSession[T](f: => T): T = f
  }

  def withDynTransaction[T](f: => T): T = OracleDatabase.withDynTransaction(f)

  def runWithRollback[T](f: => T): T = {
    Database.forDataSource(OracleDatabase.ds).withDynTransaction {
      val t = f
      dynamicSession.rollback()
      t
    }
  }

  val mockVVHClient = MockitoSugar.mock[VVHClient]
  val mockVVHRoadLinkClient = MockitoSugar.mock[VVHRoadLinkClient]
  val mockVVHComplementaryClient = MockitoSugar.mock[VVHComplementaryClient]
  val mockVVHSuravageClient = MockitoSugar.mock[VVHSuravageClient]
  val mockVVHHistoryClient = MockitoSugar.mock[VVHHistoryClient]
  val mockVVHFrozenTimeRoadLinkClient = MockitoSugar.mock[VVHFrozenTimeRoadLinkClientServicePoint]
  val mockRoadLinkService = MockitoSugar.mock[RoadLinkService]
  val mockRoadAddressDAO = MockitoSugar.mock[RoadAddressDAO]

  /**
    * TODO Fix this so that it will roll back the changes made in database.
    * Now Road_Address table is cleared and populated with the test data.
    */
  ignore("Should not have missing road addresses") {
    val vvhRoadLinks = List(
      VVHRoadlink(6656730L, 91, List(Point(0.0, 0.0), Point(120.0, 0.0)), Municipality, TrafficDirection.BothDirections, FeatureClass.AllOthers)
    )
    when(mockVVHClient.complementaryData).thenReturn(mockVVHComplementaryClient)
    when(mockVVHClient.roadLinkData).thenReturn(mockVVHRoadLinkClient)
    when(mockVVHClient.suravageData).thenReturn(mockVVHSuravageClient)
    when(mockVVHClient.historyData).thenReturn(mockVVHHistoryClient)
    when(mockVVHClient.frozenTimeRoadLinkData)thenReturn(mockVVHFrozenTimeRoadLinkClient)
    when(mockVVHRoadLinkClient.fetchByLinkIds(any[Set[Long]])).thenReturn(vvhRoadLinks)
    when(mockVVHComplementaryClient.fetchByLinkIds(any[Set[Long]])).thenReturn(vvhRoadLinks)
    when(mockVVHFrozenTimeRoadLinkClient.fetchByLinkIds(any[Set[Long]])).thenReturn(vvhRoadLinks)
    when(mockVVHSuravageClient.fetchSuravageByLinkIds(any[Set[Long]])).thenReturn(Seq())
    when(mockVVHHistoryClient.fetchVVHRoadLinkByLinkIds(any[Set[Long]])).thenReturn(Seq())

    TestTransactions.runWithRollback() {
      val roadsToBeConverted = Seq(
        //                    TIE AOSA  AJR JATKUU AET LET   ALKU LOPPU ALKUPVM                LOPPUPVM               MUUTOSPVM              -     ELY TIETYYPPI -, LINKID    KAYTTAJA      ALKUX             ALKUY              LOPPUX            LOPPUY             (LRMID)        AJORATAID  SIDE_CODE
        ConversionRoadAddress(25, 756,  22, 5,     1,  765,  62,  71,   Some(d("01.03.2016")), None,                  Some(d("30.03.2016")), None, 1,  1,        0, 6656730L, "ajrpilkont", Some(346769.646), Some(6688615.011), Some(346862.556), Some(6688687.082), 74032756001L,  7465456,   Unknown),
        ConversionRoadAddress(25, 765,  22, 5,     1,  810,  71,  116,  Some(d("01.03.2016")), None,                  Some(d("30.03.2016")), None, 1,  1,        0, 6656730L, "ajrpilkont", Some(346769.646), Some(6688615.011), Some(346862.556), Some(6688687.082), 214689473001L, 148122173, Unknown),
        ConversionRoadAddress(25, 694,  22, 5,     1,  756,  0,   62,   Some(d("01.03.2016")), None,                  Some(d("30.03.2016")), None, 1,  1,        0, 6656730L, "ajrpilkont", Some(346769.646), Some(6688615.011), Some(346862.556), Some(6688687.082), 74033231001L,  7465931,   Unknown),
        ConversionRoadAddress(25, 694,  22, 5,     0,  756,  0,   62,   Some(d("29.10.2008")), Some(d("29.02.2016")), Some(d("08.03.2016")), None, 1,  1,        0, 6656730L, "ajrpilkont", Some(346769.646), Some(6688615.011), Some(346862.556), Some(6688687.082), 74033231000L,  7465931,   Unknown),
        ConversionRoadAddress(25, 694,  22, 5,     1,  756,  0,   62,   Some(d("31.10.2006")), Some(d("28.10.2008")), Some(d("29.10.2008")), None, 1,  1,        0, 6656730L, "TR",         Some(346769.646), Some(6688615.011), Some(346862.556), Some(6688687.082), 74033231001L,  7465931,   Unknown),
        ConversionRoadAddress(25, 694,  22, 5,     0,  756,  0,   62,   Some(d("15.12.2005")), Some(d("30.10.2006")), Some(d("29.10.2008")), None, 1,  1,        0, 6656730L, "TR",         Some(346769.646), Some(6688615.011), Some(346862.556), Some(6688687.082), 74033231000L,  7465931,   Unknown),
        ConversionRoadAddress(25, 756,  22, 5,     0,  765,  62,  71,   Some(d("29.10.2008")), Some(d("29.02.2016")), Some(d("08.03.2016")), None, 1,  1,        0, 6656730L, "ajrpilkont", Some(346769.646), Some(6688615.011), Some(346862.556), Some(6688687.082), 74032756000L,  7465456,   Unknown),
        ConversionRoadAddress(25, 756,  22, 5,     1,  765,  62,  71,   Some(d("31.10.2006")), Some(d("28.10.2008")), Some(d("29.10.2008")), None, 1,  1,        0, 6656730L, "TR",         Some(346769.646), Some(6688615.011), Some(346862.556), Some(6688687.082), 74032756001L,  7465456,   Unknown),
        ConversionRoadAddress(53, 6221, 22, 5,     0,  6230, 62,  71,   Some(d("01.11.1963")), Some(d("31.12.1995")), Some(d("29.10.2008")), None, 1,  1,        0, 6656730L, "TR",         Some(346769.646), Some(6688615.011), Some(346862.556), Some(6688687.082), 74032756000L,  7465456,   Unknown),
        ConversionRoadAddress(25, 6221, 22, 5,     0,  6230, 62,  71,   Some(d("01.01.1996")), Some(d("14.12.2005")), Some(d("29.10.2008")), None, 1,  1,        0, 6656730L, "TR",         Some(346769.646), Some(6688615.011), Some(346862.556), Some(6688687.082), 74032756000L,  7465456,   Unknown),
        ConversionRoadAddress(25, 756,  22, 5,     0,  765,  62,  71,   Some(d("15.12.2005")), Some(d("30.10.2006")), Some(d("29.10.2008")), None, 1,  1,        0, 6656730L, "TR",         Some(346769.646), Some(6688615.011), Some(346862.556), Some(6688687.082), 74032756000L,  7465456,   Unknown),
        ConversionRoadAddress(25, 765,  22, 5,     0,  810,  71,  116,  Some(d("15.12.2005")), Some(d("29.02.2016")), Some(d("08.03.2016")), None, 1,  1,        0, 6656730L, "ajrpilkont", Some(346769.646), Some(6688615.011), Some(346862.556), Some(6688687.082), 214689473000L, 148122173, Unknown),
        ConversionRoadAddress(53, 6230, 22, 5,     0,  6275, 71,  116,  Some(d("01.11.1963")), Some(d("31.12.1995")), Some(d("08.03.2016")), None, 1,  1,        0, 6656730L, "ajrpilkont", Some(346769.646), Some(6688615.011), Some(346862.556), Some(6688687.082), 214689473000L, 148122173, Unknown),
        ConversionRoadAddress(25, 6230, 22, 5,     0,  6275, 71,  116,  Some(d("01.01.1996")), Some(d("14.12.2005")), Some(d("08.03.2016")), None, 1,  1,        0, 6656730L, "ajrpilkont", Some(346769.646), Some(6688615.011), Some(346862.556), Some(6688687.082), 214689473000L, 148122173, Unknown)
      )

      val importOptions = ImportOptions(false, false, 1510790400000L, "MOCK_CONVERSION", false)

      val roadAddressImporter = new RoadAddressImporter(null, mockVVHClient, importOptions) {
        override def fetchChunkLinkIdsFromConversionTable(): Seq[(Long, Long)] = {
          Seq((0l, 6656730l))
        }
        override def fetchRoadAddressFromConversionTable(minLinkId: Long, maxLinkId: Long, filter: String): Seq[ConversionRoadAddress] = {
          roadsToBeConverted
        }
      }

      val assetDataImporter = new AssetDataImporter {
        override def withDynTransaction(f: => Unit): Unit = f
        override def withDynSession[T](f: => T): T = f
        override def getRoadAddressImporter(conversionDatabase: DatabaseDef, vvhClient: VVHClient, importOptions: ImportOptions) = {
          roadAddressImporter
        }
      }
      assetDataImporter.importRoadAddressData(null, mockVVHClient, importOptions)

      val insertedRoadAddresses = RoadAddressDAO.fetchByLinkId(Set(6656730), true, true, true)

      insertedRoadAddresses.size should be(14)
    }
  }

  /**
    * TODO Fix this so that the database changes are rolled back.
    *
    * Calibration point 3   2   0   1   2   1   3   3
    * Road address      --o---+---+---o---+---o---o--
    * Common history    0   1   1   1   2   2   3   4
    * 100m            0   1   2   3   4   5   6   7   8
    */
  ignore("Should have calibration points where common history changes") {
    val vvhRoadLinks = List(
      VVHRoadlink(1000L, 91, List(Point(0.0, 0.0), Point(100.0, 0.0)), Municipality, TrafficDirection.BothDirections, FeatureClass.AllOthers)
    )
    when(mockVVHClient.complementaryData).thenReturn(mockVVHComplementaryClient)
    when(mockVVHClient.roadLinkData).thenReturn(mockVVHRoadLinkClient)
    when(mockVVHClient.suravageData).thenReturn(mockVVHSuravageClient)
    when(mockVVHClient.historyData).thenReturn(mockVVHHistoryClient)
    when(mockVVHClient.frozenTimeRoadLinkData)thenReturn(mockVVHFrozenTimeRoadLinkClient)
    when(mockVVHRoadLinkClient.fetchByLinkIds(any[Set[Long]])).thenReturn(vvhRoadLinks)
    when(mockVVHComplementaryClient.fetchByLinkIds(any[Set[Long]])).thenReturn(vvhRoadLinks)
    when(mockVVHFrozenTimeRoadLinkClient.fetchByLinkIds(any[Set[Long]])).thenReturn(vvhRoadLinks)
    when(mockVVHSuravageClient.fetchSuravageByLinkIds(any[Set[Long]])).thenReturn(Seq())
    when(mockVVHHistoryClient.fetchVVHRoadLinkByLinkIds(any[Set[Long]])).thenReturn(Seq())

    val expectedCalibrationPointValuesForAET = List(
      (1000, 3),
      (1001, 3),
      (1100, 2),
      (1101, 2),
      (1200, 0),
      (1201, 0),
      (1300, 1),
      (1301, 1),
      (1400, 2),
      (1401, 2),
      (1500, 1),
      (1501, 1),
      (1600, 3),
      (1601, 3),
      (1700, 3),
      (1701, 3)
    )

    /* TODO Make own test for these
     val expectedCalibrationPointsForAET = Map(
      1000 -> (Some(CalibrationPoint(1000L, 1000, 0)),   Some(CalibrationPoint(1000L, 1100, 100))),
      1100 -> (Some(CalibrationPoint(1000L, 1100, 100)), None                                    ),
      1200 -> (None                                    , None                                    ),
      1300 -> (None                                    , Some(CalibrationPoint(1000L, 1400, 400))),
      1400 -> (Some(CalibrationPoint(1000L, 1400, 400)), None                                    ),
      1500 -> (None                                    , Some(CalibrationPoint(1000L, 1600, 600))),
      1600 -> (Some(CalibrationPoint(1000L, 1600, 600)), Some(CalibrationPoint(1000L, 1700, 700))),
      1700 -> (Some(CalibrationPoint(1000L, 1700, 700)), Some(CalibrationPoint(1000L, 1800, 800))),
      1001 -> (Some(CalibrationPoint(1000L, 1000, 0)),   Some(CalibrationPoint(1000L, 1100, 100))),
      1101 -> (Some(CalibrationPoint(1000L, 1100, 100)), None                                    ),
      1201 -> (None                                    , None                                    ),
      1301 -> (None                                    , Some(CalibrationPoint(1000L, 1400, 400))),
      1401 -> (Some(CalibrationPoint(1000L, 1400, 400)), None                                    ),
      1501 -> (None                                    , Some(CalibrationPoint(1000L, 1600, 600))),
      1601 -> (Some(CalibrationPoint(1000L, 1600, 600)), Some(CalibrationPoint(1000L, 1700, 700))),
      1701 -> (Some(CalibrationPoint(1000L, 1700, 700)), Some(CalibrationPoint(1000L, 1800, 800)))
    )*/

    TestTransactions.runWithRollback() {
      val roadsToBeConverted = Seq(
        //                    TIE AOSA  AJR JATKUU AET   LET    ALKU LOPPU ALKUPVM                LOPPUPVM               MUUTOSPVM              -     ELY TIETYYPPI -, LINKID KAYTTAJA ALKUX             ALKUY              LOPPUX            LOPPUY             (LRMID) AJORATAID SIDE_CODE
        ConversionRoadAddress(25, 765,  22, 5,     1000, 1100,    0, 100,  Some(d("02.01.2010")), None,                  Some(d("03.01.2010")), None, 1,  1,        0, 1000L, "test",  Some(346769.646), Some(6688615.011), Some(346862.556), Some(6688687.082), 100,    10000,    Unknown),
        ConversionRoadAddress(25, 765,  22, 5,     1100, 1200,  100, 200,  Some(d("02.01.2010")), None,                  Some(d("03.01.2010")), None, 1,  1,        0, 1000L, "test",  Some(346769.646), Some(6688615.011), Some(346862.556), Some(6688687.082), 200,    11111,    Unknown),
        ConversionRoadAddress(25, 765,  22, 5,     1200, 1300,  200, 300,  Some(d("02.01.2010")), None,                  Some(d("03.01.2010")), None, 1,  1,        0, 1000L, "test",  Some(346769.646), Some(6688615.011), Some(346862.556), Some(6688687.082), 300,    11111,    Unknown),
        ConversionRoadAddress(25, 765,  22, 5,     1300, 1400,  300, 400,  Some(d("02.01.2010")), None,                  Some(d("03.01.2010")), None, 1,  1,        0, 1000L, "test",  Some(346769.646), Some(6688615.011), Some(346862.556), Some(6688687.082), 400,    11111,    Unknown),
        ConversionRoadAddress(25, 765,  22, 5,     1400, 1500,  400, 500,  Some(d("02.01.2010")), None,                  Some(d("03.01.2010")), None, 1,  1,        0, 1000L, "test",  Some(346769.646), Some(6688615.011), Some(346862.556), Some(6688687.082), 500,    22222,    Unknown),
        ConversionRoadAddress(25, 765,  22, 5,     1500, 1600,  500, 600,  Some(d("02.01.2010")), None,                  Some(d("03.01.2010")), None, 1,  1,        0, 1000L, "test",  Some(346769.646), Some(6688615.011), Some(346862.556), Some(6688687.082), 600,    22222,    Unknown),
        ConversionRoadAddress(25, 765,  22, 5,     1600, 1700,  600, 700,  Some(d("02.01.2010")), None,                  Some(d("03.01.2010")), None, 1,  1,        0, 1000L, "test",  Some(346769.646), Some(6688615.011), Some(346862.556), Some(6688687.082), 700,    33333,    Unknown),
        ConversionRoadAddress(25, 765,  22, 5,     1700, 1800,  700, 800,  Some(d("02.01.2010")), None,                  Some(d("03.01.2010")), None, 1,  1,        0, 1000L, "test",  Some(346769.646), Some(6688615.011), Some(346862.556), Some(6688687.082), 800,    44444,    Unknown),
        ConversionRoadAddress(26, 765,  22, 5,     1001, 1101,    0, 100,  Some(d("02.01.2000")), Some(d("01.01.2010")), Some(d("03.01.2010")), None, 1,  1,        0, 1000L, "test",  Some(346769.646), Some(6688615.011), Some(346862.556), Some(6688687.082), 100,    10000,    Unknown),
        ConversionRoadAddress(26, 765,  22, 5,     1101, 1201,  100, 200,  Some(d("02.01.2000")), Some(d("01.01.2010")), Some(d("03.01.2010")), None, 1,  1,        0, 1000L, "test",  Some(346769.646), Some(6688615.011), Some(346862.556), Some(6688687.082), 200,    11111,    Unknown),
        ConversionRoadAddress(26, 765,  22, 5,     1201, 1301,  200, 300,  Some(d("02.01.2000")), Some(d("01.01.2010")), Some(d("03.01.2010")), None, 1,  1,        0, 1000L, "test",  Some(346769.646), Some(6688615.011), Some(346862.556), Some(6688687.082), 300,    11111,    Unknown),
        ConversionRoadAddress(26, 765,  22, 5,     1301, 1401,  300, 400,  Some(d("02.01.2000")), Some(d("01.01.2010")), Some(d("03.01.2010")), None, 1,  1,        0, 1000L, "test",  Some(346769.646), Some(6688615.011), Some(346862.556), Some(6688687.082), 400,    11111,    Unknown),
        ConversionRoadAddress(26, 765,  22, 5,     1401, 1501,  400, 500,  Some(d("02.01.2000")), Some(d("01.01.2010")), Some(d("03.01.2010")), None, 1,  1,        0, 1000L, "test",  Some(346769.646), Some(6688615.011), Some(346862.556), Some(6688687.082), 500,    22222,    Unknown),
        ConversionRoadAddress(26, 765,  22, 5,     1501, 1601,  500, 600,  Some(d("02.01.2000")), Some(d("01.01.2010")), Some(d("03.01.2010")), None, 1,  1,        0, 1000L, "test",  Some(346769.646), Some(6688615.011), Some(346862.556), Some(6688687.082), 600,    22222,    Unknown),
        ConversionRoadAddress(26, 765,  22, 5,     1601, 1701,  600, 700,  Some(d("02.01.2000")), Some(d("01.01.2010")), Some(d("03.01.2010")), None, 1,  1,        0, 1000L, "test",  Some(346769.646), Some(6688615.011), Some(346862.556), Some(6688687.082), 700,    33333,    Unknown),
        ConversionRoadAddress(26, 765,  22, 5,     1701, 1801,  700, 800,  Some(d("02.01.2000")), Some(d("01.01.2010")), Some(d("03.01.2010")), None, 1,  1,        0, 1000L, "test",  Some(346769.646), Some(6688615.011), Some(346862.556), Some(6688687.082), 800,    44444,    Unknown)
      )

      val importOptions = ImportOptions(false, false, 1510790400000L, "MOCK_CONVERSION", false)

      val roadAddressImporter = new RoadAddressImporter(null, mockVVHClient, importOptions) {
        override def fetchChunkLinkIdsFromConversionTable(): Seq[(Long, Long)] = {
          Seq((0l, 1000l))
        }
        override def fetchRoadAddressFromConversionTable(minLinkId: Long, maxLinkId: Long, filter: String): Seq[ConversionRoadAddress] = {
          roadsToBeConverted
        }
      }

      val assetDataImporter = new AssetDataImporter {
        override def withDynTransaction(f: => Unit): Unit = f
        override def withDynSession[T](f: => T): T = f
        override def getRoadAddressImporter(conversionDatabase: DatabaseDef, vvhClient: VVHClient, importOptions: ImportOptions) = {
          roadAddressImporter
        }
      }
      assetDataImporter.importRoadAddressData(null, mockVVHClient, importOptions)

      val insertedRoadAddresses = RoadAddressDAO.fetchByLinkId(Set(1000), true, true, true)

      insertedRoadAddresses.size should be(16)

      val roadAddressIds = insertedRoadAddresses.map(_.id).mkString(", ")
      val calibrationPoints = sql"""select start_addr_m, calibration_points from road_address where id in (#${roadAddressIds}) order by start_addr_m""".as[(Long, Long)].list
      calibrationPoints should equal(expectedCalibrationPointValuesForAET)

      // TODO Make own test for these
      // insertedRoadAddresses.foldLeft(Map.empty[Long, (Option[CalibrationPoint], Option[CalibrationPoint])])((map, ra) => map + (ra.startAddrMValue -> ra.calibrationPoints)) should equal(expectedCalibrationPointsForAET)
    }
  }

  test("if the geometry is a loop then it should be updated") {

    val roadNumber = 9999999
    val roadPartNumber = 1
    val linkId = 12345L
    val commonHistoryId = 123
    val geom1 = Seq(Point(9.9, 10.1), Point(20.0, 20.0))
    val geom2 = Seq(Point(20.1, 20.1), Point(9.9, 10.1))
    val vvhGeom = Seq(Point(40.0, 40.0, 40.0), Point(60.0, 60.0, 60.0))
    val segmentStartMValue = 0.0
    val segmentEndMValue = 10.0

    runWithRollback {
      //Road Objects
      val ra = Seq(RoadAddress(RoadAddressDAO.getNextRoadAddressId, roadNumber, roadPartNumber, RoadType.PublicRoad, Track.Combined, Discontinuous, 0L, 10L,
        Some(DateTime.parse("1901-01-01")), None, Option("tester"), 0, linkId, segmentStartMValue, segmentEndMValue, SideCode.TowardsDigitizing, 0, (None, None), false,
        geom1, LinkGeomSource.NormalLinkInterface, 8, NoTermination, commonHistoryId),
        RoadAddress(RoadAddressDAO.getNextRoadAddressId, roadNumber, roadPartNumber, RoadType.PublicRoad, Track.Combined, Discontinuous, 10L, 20L,
          Some(DateTime.parse("1901-01-01")), None, Option("tester"), 0, linkId, segmentStartMValue, segmentEndMValue, SideCode.TowardsDigitizing, 0, (None, None), false,
          geom2, LinkGeomSource.NormalLinkInterface, 8, NoTermination, commonHistoryId))
      val vvhRoadLinks = List(
        VVHRoadlink(linkId, 91, vvhGeom, Municipality, TrafficDirection.BothDirections, FeatureClass.AllOthers)
      )
      val roadlink = RoadLink(linkId, Seq(Point(10.0, 10.0, 10.0), Point(20.0, 20.0))
        , 10.0, State, 99, TrafficDirection.AgainstDigitizing, UnknownLinkType, Some("25.06.2015 03:00:00"), Some("vvh_modified"), Map("MUNICIPALITYCODE" -> BigInt.apply(749)),
        InUse, NormalLinkInterface)
      //Set up mocked data
      when(mockVVHClient.complementaryData).thenReturn(mockVVHComplementaryClient)
      when(mockVVHClient.complementaryData.fetchByLinkIds(any[Set[Long]])).thenReturn(Seq.empty[VVHRoadlink])
      when(mockVVHClient.roadLinkData).thenReturn(mockVVHRoadLinkClient)
      when(mockVVHClient.roadLinkData.fetchByLinkIds(any[Set[Long]])).thenReturn(vvhRoadLinks)
      when(mockVVHClient.suravageData).thenReturn(mockVVHSuravageClient)
      when(mockVVHClient.suravageData.fetchSuravageByLinkIds(any[Set[Long]])).thenReturn(Seq.empty[VVHRoadlink])
      when(mockVVHClient.historyData).thenReturn(mockVVHHistoryClient)
      when(mockVVHClient.frozenTimeRoadLinkData) thenReturn (mockVVHFrozenTimeRoadLinkClient)

      RoadAddressDAO.create(ra)
      val addressesBeforeUpdate = RoadAddressDAO.fetchByLinkId(Set(linkId)).sortBy(_.endAddrMValue)
      addressesBeforeUpdate.head.geometry.equals(geom1) should be(true)
      addressesBeforeUpdate.last.geometry.equals(geom2) should be(true)
      assetDataImporter.updateRoadAddressesGeometry(mockVVHClient, false, s"AND ROAD_NUMBER = $roadNumber")
      val suposedGeom = GeometryUtils.truncateGeometry3D(vvhGeom, segmentStartMValue, segmentEndMValue).map(g => {
        val roundedX = BigDecimal(g.x).setScale(3, BigDecimal.RoundingMode.HALF_UP).toDouble
        val roundedY = BigDecimal(g.y).setScale(3, BigDecimal.RoundingMode.HALF_UP).toDouble
        val roundedZ = 0.0
        Point(roundedX, roundedY, roundedZ)
      })
      val addressesAfterUpdate = RoadAddressDAO.fetchByLinkId(Set(linkId)).sortBy(_.endAddrMValue)
      addressesAfterUpdate.head.geometry.equals(suposedGeom) should be(true)
      addressesAfterUpdate.last.geometry.equals(suposedGeom) should be(true)
    }

  }

  val dateTimeFormatter = DateTimeFormat.forPattern("dd.MM.yyyy")

  def d(date: String): DateTime = {DateTime.parse(date, dateTimeFormatter)}

}
