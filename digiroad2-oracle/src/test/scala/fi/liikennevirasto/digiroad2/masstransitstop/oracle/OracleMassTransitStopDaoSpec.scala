package fi.liikennevirasto.digiroad2.masstransitstop.oracle

import fi.liikennevirasto.digiroad2.masstransitstop.oracle.Queries.PropertyRow
import fi.liikennevirasto.digiroad2.asset.{AdministrativeClass, Modification}
import fi.liikennevirasto.digiroad2.oracle.OracleDatabase
import fi.liikennevirasto.digiroad2.{MassTransitStopRow, Point}
import org.scalatest.{FunSuite, MustMatchers}

import slick.driver.JdbcDriver.backend.Database.dynamicSession
import slick.jdbc.StaticQuery.interpolation
import slick.jdbc.{StaticQuery => Q}

class OracleMassTransitStopDaoSpec extends FunSuite with MustMatchers {
  val massTransitStopDao = new MassTransitStopDao

  def withDynTransaction[T](f: => T): T = OracleDatabase.withDynTransaction(f)

  test("bearing description is correct") {
    massTransitStopDao.getBearingDescription(2, Some(316)) must equal("Pohjoinen")
    massTransitStopDao.getBearingDescription(2, Some(45)) must equal("Pohjoinen")
    massTransitStopDao.getBearingDescription(2, Some(46)) must equal("Itä")
    massTransitStopDao.getBearingDescription(2, Some(135)) must equal("Itä")
    massTransitStopDao.getBearingDescription(2, Some(136)) must equal("Etelä")
    massTransitStopDao.getBearingDescription(2, Some(225)) must equal("Etelä")
    massTransitStopDao.getBearingDescription(2, Some(226)) must equal("Länsi")
    massTransitStopDao.getBearingDescription(2, Some(315)) must equal("Länsi")
  }

  test("bearing description is correct when validity direction is against") {
    massTransitStopDao.getBearingDescription(3, Some(316)) must equal("Etelä")
    massTransitStopDao.getBearingDescription(3, Some(45)) must equal("Etelä")
    massTransitStopDao.getBearingDescription(3, Some(46)) must equal("Länsi")
    massTransitStopDao.getBearingDescription(3, Some(135)) must equal("Länsi")
    massTransitStopDao.getBearingDescription(3, Some(136)) must equal("Pohjoinen")
    massTransitStopDao.getBearingDescription(3, Some(225)) must equal("Pohjoinen")
    massTransitStopDao.getBearingDescription(3, Some(226)) must equal("Itä")
    massTransitStopDao.getBearingDescription(3, Some(315)) must equal("Itä")
  }

  test("bearing property row generates bearing description") {
    val propertyRow = PropertyRow(1, "liikennointisuuntima", "", false, "", "")
    val properties = massTransitStopDao.assetRowToProperty(List(createAssetRow(propertyRow)))
    properties.head.publicId must equal("liikennointisuuntima")
    properties.head.values.head.propertyDisplayValue must equal(Some("Etelä"))
  }

  test("asset row values are mapped correctly to property row") {
    val propertyRow = PropertyRow(1, "sometestproperty", "", false, "123", "foo")
    val properties = massTransitStopDao.assetRowToProperty(List(createAssetRow(propertyRow)))
    properties.head.publicId must equal("sometestproperty")
    properties.head.values.head.propertyDisplayValue must equal(Some("foo"))
    properties.head.values.head.propertyValue must equal("123")
  }

  test("get property descriptions correctly"){
    withDynTransaction {
      val c = sql""" Select p.public_id, p.property_type, e.value From Property p Left Join Enumerated_value e On p.id = e.property_id
        """.as[(String,String,String)].list
      val singleChoiceList = c.filter(p =>
        p._2 == "single_choice")

      val descs = massTransitStopDao.getPropertyDescription(singleChoiceList.head._1, singleChoiceList.head._3)
      descs.length must be > (0)
    }
  }

  private def createAssetRow(propertyRow: PropertyRow) = {
    MassTransitStopRow(1, 1, 1, Some(Point(1, 1)), 123l, Some(180), 2, None, None, propertyRow,
      Modification(None, None), Modification(None, None), Some(Point(1, 1)), lrmPosition = null, AdministrativeClass.apply(99), 235, false)
  }
}
