package fi.liikennevirasto.viite

import java.sql.{SQLException, SQLIntegrityConstraintViolationException}

import fi.liikennevirasto.digiroad2.dao.Sequences
import fi.liikennevirasto.digiroad2.{GeometryUtils, Point}
import fi.liikennevirasto.digiroad2.oracle.OracleDatabase
import fi.liikennevirasto.viite.AddressConsistencyValidator.AddressError.{InconsistentTopology, OverlappingRoadAddresses}
import fi.liikennevirasto.viite.dao._
import fi.liikennevirasto.digiroad2.util.Track
import fi.liikennevirasto.digiroad2.util.Track.Combined
import fi.liikennevirasto.viite.AddressConsistencyValidator.AddressError
import fi.liikennevirasto.viite.process.RoadwayAddressMapper
import org.joda.time.DateTime
import org.slf4j.LoggerFactory
import slick.driver.JdbcDriver.backend.Database.dynamicSession



class RoadNetworkService {

  def withDynTransaction[T](f: => T): T = OracleDatabase.withDynTransaction(f)

  def withDynSession[T](f: => T): T = OracleDatabase.withDynSession(f)

  val logger = LoggerFactory.getLogger(getClass)
  val roadNetworkDAO = new RoadNetworkDAO
  val roadwayDAO = new RoadwayDAO
  val linearLocationDAO = new LinearLocationDAO
  val roadwayAddressMapper = new RoadwayAddressMapper(roadwayDAO, linearLocationDAO)

  def checkRoadAddressNetwork(options: RoadCheckOptions): Unit = {
    val currNetworkVersion = roadNetworkDAO.getLatestRoadNetworkVersionId

    def checkRoadways(l: Seq[Roadway], r: Seq[Roadway]): Seq[RoadNetworkError] = {

      val sectionLeft = l.zip(l.tail).foldLeft(Seq.empty[RoadNetworkError])((errors, rws) =>
          checkAddressMValues(rws._1, rws._2, errors)
        )

      val sectionRight = r.zip(r.tail).foldLeft(Seq.empty[RoadNetworkError])((errors, rws) =>
        checkAddressMValues(rws._1, rws._2, errors)
      )

      sectionLeft ++ sectionRight
    }

    def checkTwoTrackLinearLocations(mapped: Map[Long, Option[Seq[LinearLocation]]]): Seq[RoadNetworkError] = {
      //TODO add test for this with empty Seq[LinearLocation] for any given roadwayId key
      val allLocations = mapped.values.flatten.flatten.toSeq
        if(allLocations.isEmpty)
          Seq.empty[RoadNetworkError]
        else {
        val errors: Seq[RoadNetworkError] = mapped.flatMap { case (roadwayId, locations) =>
          val locationsError: Seq[LinearLocation] = locations.get.filter(loc =>
            (!allLocations.exists(l => (l.calibrationPoints._1 == loc.calibrationPoints._1) && l.id != loc.id)
              || !allLocations.exists(l => (l.calibrationPoints._2 == loc.calibrationPoints._2) && l.id != loc.id))
          )
          locationsError.map { loc =>
            RoadNetworkError(Sequences.nextRoadNetworkError, roadwayId, loc.id, AddressError.InconsistentTopology, System.currentTimeMillis(), currNetworkVersion)
          }
        }.toSeq
        errors
      }
    }

    def checkCombinedLinearLocations(mapped: Map[Long, Option[Seq[LinearLocation]]]): Seq[RoadNetworkError] = {
      //TODO add test for this with empty Seq[LinearLocation] for any given roadwayId key
      val allLocations = mapped.values.flatten.flatten.toSeq
        if(allLocations.isEmpty)
          Seq.empty[RoadNetworkError]
        else {
          val sortedLocations = allLocations.sortBy(_.orderNumber)
          if (sortedLocations.isEmpty)
            logger.info(s"WARNING!!!!!!! Empty linear locations for some of those roadway ids ${mapped.keys.mkString(",")}")
          val (first, last) = (sortedLocations.head, sortedLocations.last)

          val errors: Seq[RoadNetworkError] = mapped.flatMap { case (roadwayId, locations) =>
            val locationsError: Seq[LinearLocation] = locations.get.filter(loc =>
              !allLocations.exists(l => (l.calibrationPoints._2 == loc.calibrationPoints._1) && l.id != loc.id && l.id != last.id)
            )
            locationsError.map { loc =>
              RoadNetworkError(Sequences.nextRoadNetworkError, roadwayId, loc.id, AddressError.InconsistentTopology, System.currentTimeMillis(), currNetworkVersion)
            }
          }.toSeq
          errors
        }
    }

    def checkAddressMValues(rw1: Roadway, rw2: Roadway, errors: Seq[RoadNetworkError]): Seq[RoadNetworkError] = {
      rw1.endAddrMValue != rw2.startAddrMValue match{
        case true => {
            errors :+ RoadNetworkError(Sequences.nextRoadNetworkError, rw1.id, 0L, AddressError.InconsistentTopology, System.currentTimeMillis(), currNetworkVersion)
          }
        case _ => Seq()
      }
    }

//    withDynTransaction {
      try {
        val allRoads = roadwayDAO.fetchAllByRoadNumbers(options.roadNumbers)
        val roadways = allRoads.groupBy(g => (g.roadNumber, g.roadPartNumber, g.endDate))
        roadways.par.foreach { group =>
          val (section, roadway) = group

          val (combinedLeft, combinedRight) = (roadway.filter(t => t.track != Track.RightSide).sortBy(_.startAddrMValue), roadway.filter(t => t.track != Track.LeftSide).sortBy(_.startAddrMValue))
          val roadwaysErrors = checkRoadways(combinedLeft, combinedRight)
          logger.info(s" Found ${roadwaysErrors.size} roadway errors for RoadNumber ${section._1} and Part ${section._2}")

          //terminated roadways dont have linear locations
          val (combinedRoadways, twoTrackRoadways) = roadway.filter(_.terminated == TerminationCode.NoTermination).partition(_.track == Combined)

          logger.info(s" start of fetch of linear locations")
          val combinedLocations = linearLocationDAO.fetchByRoadways(combinedRoadways.map(_.roadwayNumber).toSet).groupBy(_.roadwayNumber)
          val twoTrackLocations = linearLocationDAO.fetchByRoadways(twoTrackRoadways.map(_.roadwayNumber).toSet).groupBy(_.roadwayNumber)

          val mappedCombined = combinedRoadways.map(r => r.id -> combinedLocations.get(r.roadwayNumber)).toMap
          val mappedTwoTrack = twoTrackRoadways.map(r => r.id -> twoTrackLocations.get(r.roadwayNumber)).toMap

          logger.info(s" start checking of linear locations")
          val twoTrackErrors = checkTwoTrackLinearLocations(mappedTwoTrack)
          val combinedErrors = checkCombinedLinearLocations(mappedCombined)
          val linearLocationErrors = twoTrackErrors ++ combinedErrors
          logger.info(s" Found ${linearLocationErrors.size} linear locations errors for RoadNumber ${section._1} and Part ${section._2} (twoTrack: ${twoTrackErrors.size}) , (combined: ${combinedErrors.size})")

          (roadwaysErrors ++ linearLocationErrors).foreach { e =>
            logger.info(s" Found error for roadway id ${e.roadwayId}, linear location id ${e.linearLocationId}")
            roadNetworkDAO.addRoadNetworkError(e.roadwayId, e.linearLocationId, InconsistentTopology)
          }
        }

        //  .groupBy(_.roadNumber).flatMap(road => {
        //          val groupedRoadParts = road._2.groupBy(_.roadPartNumber).toSeq.sortBy(_._1)
        //          val lastRoadAddress = groupedRoadParts.last._2.maxBy(_.startAddrMValue)
        //          groupedRoadParts.map(roadPart => {
        //            if (roadPart._2.last.roadPartNumber == lastRoadAddress.roadPartNumber && lastRoadAddress.discontinuity != Discontinuity.EndOfRoad) {
        //              RoadNetworkDAO.addRoadNetworkError(lastRoadAddress.id, InconsistentTopology.value)
        //            }
        //            val sortedRoads = roadPart._2.sortBy(s => (s.track.value, s.startAddrMValue))
        //            sortedRoads.zip(sortedRoads.tail).foreach(r => {
        //              checkOverlapping(r._1, r._2)(sortedRoads)
        //              checkTopology(r._1, r._2)(sortedRoads)
        //            })
        //            roadPart
        //          })
        //        })
        //        if (!RoadNetworkDAO.hasRoadNetworkErrors) {
        //          RoadNetworkDAO.expireRoadNetwork
        //          RoadNetworkDAO.createPublishedRoadNetwork
        //          allRoads.foreach(r => r._2.foreach(p => RoadNetworkDAO.createPublishedRoadway(RoadNetworkDAO.getLatestRoadNetworkVersion.get, p.id)))
        //        }
        //        ExportLockDAO.delete
        //      } catch {
        //        case e: SQLIntegrityConstraintViolationException => logger.info("A road network check is already running")
        //        case _: Exception => {
        //          logger.error("Error during road address network check")
        //          dynamicSession.rollback()
        //          ExportLockDAO.delete
        //        }
        } catch {
                  case e: SQLIntegrityConstraintViolationException => logger.error("A road network check is already running")
                  case e: SQLException => {
                    logger.info("SQL Exception")
                    logger.error(e.getMessage)
                    dynamicSession.rollback()
                  }
                  case e: Exception =>{
                    logger.error(e.getMessage)
                    dynamicSession.rollback()
                  }
        }
//      }
    }

  def getLatestPublishedNetworkDate : Option[DateTime] = {
    withDynSession {
      roadNetworkDAO.getLatestPublishedNetworkDate
    }
  }
}
case class RoadCheckOptions(roadways: Seq[Long], roadNumbers: Seq[Long])
