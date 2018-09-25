package fi.liikennevirasto.digiroad2.dao

import fi.liikennevirasto.digiroad2.dao.Queries._
import slick.driver.JdbcDriver.backend.Database
import Database.dynamicSession

object Sequences {

  def nextViitePrimaryKeySeqValue: Long = {
    nextViitePrimaryKeyId.as[Long].first
  }

  def nextRoadwayId: Long = {
    Queries.nextRoadwayId.as[Long].first
  }

  def nextLinearLocationId: Long = {
    Queries.nextLinearLocationId.as[Long].first
  }

  def fetchViitePrimaryKeySeqValues(len: Int): List[Long] = {
    fetchViitePrimaryKeyId(len)
  }

  def fetchRoadwayIds(len: Int): List[Long] = {
    Queries.fetchRoadwayIds(len)
  }

  def fetchLinearLocationIds(len: Int): List[Long] = {
    Queries.fetchLinearLocationIds(len)
  }

  def nextRoadwaySeqValue: Long = {
    nextRoadwayNumber.as[Long].first
  }

  def nextRoadNetworkErrorSeqValue: Long = {
    nextRoadNetworkErrorId.as[Long].first
  }
}
