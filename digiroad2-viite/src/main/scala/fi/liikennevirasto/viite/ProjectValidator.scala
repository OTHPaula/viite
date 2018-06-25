package fi.liikennevirasto.viite

import fi.liikennevirasto.digiroad2.{GeometryUtils, Point}
import fi.liikennevirasto.digiroad2.asset.BoundingRectangle
import fi.liikennevirasto.digiroad2.asset.SideCode.{AgainstDigitizing, TowardsDigitizing}
import fi.liikennevirasto.digiroad2.linearasset.RoadLinkLike
import fi.liikennevirasto.digiroad2.util.Track
import fi.liikennevirasto.digiroad2.util.Track.{Combined, LeftSide, RightSide}
import fi.liikennevirasto.viite.dao.Discontinuity.{MinorDiscontinuity, _}
import fi.liikennevirasto.viite.dao.LinkStatus._
import fi.liikennevirasto.viite.dao._
import fi.liikennevirasto.viite.model.RoadAddressLink
import fi.liikennevirasto.viite.process.TrackSectionOrder
import org.slf4j.LoggerFactory
import fi.liikennevirasto.digiroad2.util.LogUtils.time

object ProjectValidator {

  val logger = LoggerFactory.getLogger(getClass)

  private def distanceToPoint = 10.0

  // Utility method, will return correct GeometryEndpoint
  private def endPoint(b: BaseRoadAddress) = {
    b.sideCode match {
      case TowardsDigitizing => b.geometry.last
      case AgainstDigitizing => b.geometry.head
      case _ => Point(0.0, 0.0)
    }
  }

  private def startPoint(b: BaseRoadAddress) = {
    b.sideCode match {
      case TowardsDigitizing => b.geometry.head
      case AgainstDigitizing => b.geometry.last
      case _ => Point(0.0, 0.0)
    }
  }

  private def connected(pl1: BaseRoadAddress, pl2: BaseRoadAddress) = {
    val connectingPoint = pl1.sideCode match {
      case AgainstDigitizing => pl1.geometry.head
      case _ => pl1.geometry.last
    }
    GeometryUtils.areAdjacent(pl2.geometry, connectingPoint, fi.liikennevirasto.viite.MaxDistanceForConnectedLinks)
  }

  sealed trait ValidationError {
    def value: Int

    def message: String

    def notification: Boolean
  }

  object ValidationErrorList {
    val values = Set(MinorDiscontinuityFound, MajorDiscontinuityFound, InsufficientTrackCoverage, DiscontinuousAddressScheme,
      SharedLinkIdsExist, NoContinuityCodesAtEnd, UnsuccessfulRecalculation, MissingEndOfRoad, HasNotHandledLinks, ConnectedDiscontinuousLink,
      IncompatibleDiscontinuityCodes, EndOfRoadNotOnLastPart, ElyCodeChangeDetected, DiscontinuityOnRamp,
      ErrorInValidationOfUnchangedLinks, RoadNotEndingInElyBorder, RoadContinuesInAnotherEly)

    // Viite-942
    case object MissingEndOfRoad extends ValidationError {
      def value = 0

      def message: String = MissingEndOfRoadMessage

      def notification = false
    }

    // Viite-453
    // There must be a minor discontinuity if the jump is longer than 0.1 m (10 cm) between road links
    case object MinorDiscontinuityFound extends ValidationError {
      def value = 1

      def message: String = MinorDiscontinuityFoundMessage

      def notification = true
    }

    // Viite-453
    // There must be a major discontinuity if the jump is longer than 50 meters
    case object MajorDiscontinuityFound extends ValidationError {
      def value = 2

      def message: String = MajorDiscontinuityFoundMessage

      def notification = false
    }

    // Viite-453
    // For every track 1 there must exist track 2 that covers the same address span and vice versa
    case object InsufficientTrackCoverage extends ValidationError {
      def value = 3

      def message: String = InsufficientTrackCoverageMessage

      def notification = false
    }

    // Viite-453
    // There must be a continuous road addressing scheme so that all values from 0 to the highest number are covered
    case object DiscontinuousAddressScheme extends ValidationError {
      def value = 4

      def message: String = DiscontinuousAddressSchemeMessage

      def notification = false
    }

    // Viite-453
    // There are no link ids shared between the project and the current road address + lrm_position tables at the project date (start_date, end_date)
    case object SharedLinkIdsExist extends ValidationError {
      def value = 5

      def message: String = SharedLinkIdsExistMessage

      def notification = false
    }

    // Viite-453
    // Continuity codes are given for end of road
    case object NoContinuityCodesAtEnd extends ValidationError {
      def value = 6

      def message: String = NoContinuityCodesAtEndMessage

      def notification = false
    }

    // Viite-453
    // Recalculation of M values and delta calculation are both unsuccessful for every road part in project
    case object UnsuccessfulRecalculation extends ValidationError {
      def value = 7

      def message: String = UnsuccessfulRecalculationMessage

      def notification = false
    }

    case object HasNotHandledLinks extends ValidationError {
      def value = 8

      def message: String = ""

      def notification = false
    }

    case object ConnectedDiscontinuousLink extends ValidationError {
      def value = 9

      def message: String = ConnectedDiscontinuousMessage

      def notification = false
    }

    case object IncompatibleDiscontinuityCodes extends ValidationError {
      def value = 10

      def message: String = DifferingDiscontinuityCodesForTracks

      def notification = false
    }

    case object EndOfRoadNotOnLastPart extends ValidationError {
      def value = 11

      def message: String = EndOfRoadNotOnLastPartMessage

      def notification = false
    }

    case object ElyCodeChangeDetected extends ValidationError {
      def value = 12

      def message: String = ElyCodeChangeNotPresent

      def notification = false
    }

    case object DiscontinuityOnRamp extends ValidationError {
      def value = 13

      def message: String = RampDiscontinuityFoundMessage

      def notification = true
    }

    // Viite-473
    // Unchanged project links cannot have any other operation (transfer, termination) previously on the same number and part
    case object ErrorInValidationOfUnchangedLinks extends ValidationError {
      def value = 14

      def message: String = ErrorInValidationOfUnchangedLinksMessage

      def notification = false
    }

    case object RoadConnectingRoundabout extends ValidationError {
      def value = 15

      def message: String = MinorDiscontinuousWhenRoadConnectingRoundabout

      def notification = false
    }

    case object RoadNotEndingInElyBorder extends ValidationError {
      def value = 16

      def message: String = RoadNotEndingInElyBorderMessage

      def notification = true
    }

    case object RoadContinuesInAnotherEly extends ValidationError {
      def value = 17

      def message: String = RoadContinuesInAnotherElyMessage

      def notification = true
    }

    case object TerminationContinuity extends ValidationError {
      def value = 18

      def message: String = WrongDiscontinuityWhenAdjacentToTerminatedRoad

      def notification = true
    }

    case object DoubleEndOfRoad extends ValidationError {
      def value = 19

      def message: String = DoubleEndOfRoadMessage

      def notification = true
    }

    def apply(intValue: Int): ValidationError = {
      values.find(_.value == intValue).get
    }
  }

  case class ValidationErrorDetails(projectId: Long, validationError: ValidationError,
                                    affectedIds: Seq[Long], coordinates: Seq[ProjectCoordinates],
                                    optionalInformation: Option[String])

  def error(id: Long, validationError: ValidationError, info: String = "N/A")(pl: Seq[ProjectLink]): Option[ValidationErrorDetails] = {
    val (splitLinks, nonSplitLinks) = pl.partition(_.isSplit)
    val splitedIds = splitLinks.flatMap(s => Seq(s.connectedLinkId.get, s.linkId))
    val connectedSplitLinks = ProjectDAO.getProjectLinksByConnectedLinkId(splitedIds)
    val (ids, points) = (nonSplitLinks ++ connectedSplitLinks).map(pl => (pl.id, GeometryUtils.midPointGeometry(pl.geometry))).unzip
    if (ids.nonEmpty) {
      Some(ValidationErrorDetails(id, validationError, ids,
        points.map(p => ProjectCoordinates(p.x, p.y, 12)), Some(info)))
    } else {
      None
    }
  }

  def outsideOfProjectError(id: Long, validationError: ValidationError)(pl: Seq[RoadAddress]): Option[ValidationErrorDetails] = {
    val (ids, points) = pl.map(pl => (pl.id, GeometryUtils.midPointGeometry(pl.geometry))).unzip
    if (ids.nonEmpty)
      Some(ValidationErrorDetails(id, validationError, ids,
        points.map(p => ProjectCoordinates(p.x, p.y, 12)), None))
    else
      None
  }

  def validateProject(project: RoadAddressProject, projectLinks: Seq[ProjectLink]): Seq[ValidationErrorDetails] = {

    def checkProjectContinuity(errors: Seq[ValidationErrorDetails]): Seq[ValidationErrorDetails] = {
      projectLinks.filter(_.status != Terminated).groupBy(pl => (pl.roadNumber, pl.roadPartNumber)).flatMap {
        case ((road, _), seq) =>
          if (road < RampsMinBound || road > RampsMaxBound) {
            checkRoadContinuityCodes(project, seq)
          } else {
            checkRoadContinuityCodes(project, seq, isRampValidation = true)
          }
        case _ => Seq()
      }.toSeq ++ errors
    }

    def checkForNotHandledLinks(errors: Seq[ValidationErrorDetails]): Seq[ValidationErrorDetails] = {
      val notHandled = projectLinks.filter(_.status == LinkStatus.NotHandled)
      notHandled.groupBy(link => (link.roadNumber, link.roadPartNumber)).foldLeft(Seq.empty[ValidationErrorDetails])((errorDetails, road) =>
        errorDetails :+ ValidationErrorDetails(project.id, ValidationErrorList.HasNotHandledLinks,
          Seq(road._2.size), road._2.map { l =>
            val point = GeometryUtils.midPointGeometry(l.geometry)
            ProjectCoordinates(point.x, point.y, 12)
          },
          Some(HasNotHandledLinksMessage.format(road._2.size, road._1._1, road._1._2)))
      ) ++ errors
    }

    def checkForInvalidUnchangedLinks(errors: Seq[ValidationErrorDetails]): Seq[ValidationErrorDetails] = {
      val roadNumberAndParts = projectLinks.groupBy(pl => (pl.roadNumber, pl.roadPartNumber)).keySet
      val invalidUnchangedLinks = roadNumberAndParts.flatMap(rn => ProjectDAO.getInvalidUnchangedOperationProjectLinks(rn._1, rn._2)).toSeq
      invalidUnchangedLinks.map { projectLink =>
        val point = GeometryUtils.midPointGeometry(projectLink.geometry)
        ValidationErrorDetails(project.id, ValidationErrorList.ErrorInValidationOfUnchangedLinks,
          Seq(projectLink.id), Seq(ProjectCoordinates(point.x, point.y, 12)),
          Some("TIE : %d, OSA: %d, AET: %d".format(projectLink.roadNumber, projectLink.roadPartNumber, projectLink.startAddrMValue)))
      } ++ errors
    }

    def checkTrackCodePairing(errors: Seq[ValidationErrorDetails]): Seq[ValidationErrorDetails] = {
      checkTrackCode(project, projectLinks) ++ errors
    }

    def checkRemovedEndOfRoadPart(errors: Seq[ValidationErrorDetails]): Seq[ValidationErrorDetails] = {
      checkRemovedEndOfRoadParts(project) ++ errors
    }

    def checkElyCodeChange(errors: Seq[ValidationErrorDetails]): Seq[ValidationErrorDetails] = {
      checkProjectElyCodes(project, projectLinks) ++ errors
    }

    time(logger, "Validating project") {
      val projectValidations: Seq[Seq[ValidationErrorDetails] =>  Seq[ValidationErrorDetails]] = Seq(
        checkProjectContinuity,
        checkForNotHandledLinks,
        checkForInvalidUnchangedLinks,
        checkTrackCodePairing,
        checkRemovedEndOfRoadPart,
        checkElyCodeChange
      )

      val errors :Seq[ValidationErrorDetails] = projectValidations.foldLeft(Seq.empty[ValidationErrorDetails]) { case (errors, validation) =>
        validation(errors)
      }
      errors.distinct
    }

  }

  /**
    * Check for non-ramp and roundabout roads:
    * 1) If inside a part there is a gap between links > .1 meters, discontinuity 4 (minor) is required
    * 2) If inside a part there is no gap, discontinuity 5 (cont) is required
    * 3) End of road part, discontinuity 2 or 3 (major, ely change) is required if there is a gap
    * 4) If a part that contained end of road discontinuity is terminated / renumbered / transferred,
    * there must be a new end of road link for that road at the last part
    * 5) If the next road part has differing ely code then there must be a discontinuity code 3 at the end
    *
    * @param project Road address project
    * @param seq Project links
    * @return
    */
  def checkRoadContinuityCodes(project: RoadAddressProject, seq: Seq[ProjectLink], isRampValidation: Boolean = false): Seq[ValidationErrorDetails] = {


    def isConnectingRoundabout(pls: Seq[ProjectLink]): Boolean = {
      // This code means that this road part (of a ramp) should be connected to a roundabout
      val endPoints = pls.map(endPoint).map(p => (p.x, p.y)).unzip
      val boundingBox = BoundingRectangle(Point(endPoints._1.min,
        endPoints._2.min), Point(endPoints._1.max, endPoints._2.max))
      // Fetch all ramps and roundabouts roads and parts this is connected to (or these, if ramp has multiple links)
      val roadParts = RoadAddressDAO.fetchRoadAddressesByBoundingBox(boundingBox, fetchOnlyFloating = false, onlyNormalRoads = false,
        Seq((RampsMinBound, RampsMaxBound))).filter(ra =>
        pls.exists(pl => connected(pl, ra))).groupBy(ra => (ra.roadNumber, ra.roadPartNumber))
      // Check all the fetched road parts to see if any of them is a roundabout
      roadParts.keys.exists(rp => TrackSectionOrder.isRoundabout(
        RoadAddressDAO.fetchByRoadPart(rp._1, rp._2, includeFloating = true)))
    }

    def checkContinuityBetweenParts(errors: Seq[ValidationErrorDetails]): Seq[ValidationErrorDetails] = {
      error(project.id, ValidationErrorList.ConnectedDiscontinuousLink)(seq.sortBy(_.startAddrMValue).filterNot { pl =>
        // Check that pl is continuous or after it there is no connected project link
        val disconnectedLinks = seq.exists(pl2 => (pl2.startAddrMValue != pl.endAddrMValue || !connected(pl, pl2)) && Track.isTrackContinuous(pl2.track, pl.track))
        disconnectedLinks || pl.discontinuity == Continuous
      }).toSeq ++ errors
    }

    def checkMinorDiscontinuityBetweenLinksOnPart(errors: Seq[ValidationErrorDetails]): Seq[ValidationErrorDetails] = {
      def checkConnected(curr: ProjectLink, next: ProjectLink, sortedGroup: Seq[ProjectLink]): Option[ProjectLink] = {
        val matchingLink = curr.endAddrMValue == next.startAddrMValue && connected(curr, next)
        if(matchingLink)
          None
        else
          Some(curr)
      }
      val discontinuous: Seq[ProjectLink] = seq.groupBy(s => (s.roadNumber, s.roadPartNumber)).flatMap{ g =>
        val trackIntervals = Seq(g._2.filter(_.track != RightSide), g._2.filter(_.track != LeftSide))
        val connectedLinks: Seq[ProjectLink] = trackIntervals.flatMap{
          interval => {
            if (interval.size > 1) {
              interval.sortBy(_.startAddrMValue).sliding(2).flatMap {
                case Seq(first, second) => checkConnected(first, second, interval)
              }
            } else None
          }
        }
        connectedLinks.filterNot(c => c.discontinuity == MinorDiscontinuity)
      }.toSeq

      error(project.id, ValidationErrorList.MinorDiscontinuityFound)(discontinuous).toSeq ++ errors
    }

    def checkDiscontinuityBetweenLinksOnRamps(errors: Seq[ValidationErrorDetails]): Seq[ValidationErrorDetails] = {
      val discontinuousErrors = if (isRampValidation) {
        error(project.id, ValidationErrorList.DiscontinuityOnRamp)(seq.filter { pl =>
          // Check that pl has no discontinuity unless on last link and after it the possible project link is connected
          val nextLink = seq.find(pl2 => pl2.startAddrMValue == pl.endAddrMValue)
          (nextLink.nonEmpty && pl.discontinuity != Continuous) ||
            nextLink.exists(pl2 => !connected(pl, pl2))
        })
      }
      else None
      discontinuousErrors.toSeq ++ errors
    }

    def checkEndOfRoadOnLastPart(errors: Seq[ValidationErrorDetails]): Seq[ValidationErrorDetails] = {
      val allProjectLinks = ProjectDAO.getProjectLinks(project.id)
      val afterCheckErrors = seq.groupBy(_.roadNumber).flatMap { g =>
        val validRoadParts = RoadAddressDAO.getValidRoadParts(g._1.toInt, project.startDate)
        val trackIntervals = Seq(g._2.filter(_.track != RightSide), g._2.filter(_.track != LeftSide))
        trackIntervals.flatMap {
          interval =>
            val nonTerminated = interval.filter(r => r.status != LinkStatus.Terminated)
            if(nonTerminated.nonEmpty){
              val last = nonTerminated.maxBy(_.endAddrMValue)
              val (road, part) = (last.roadNumber, last.roadPartNumber)
              val discontinuity = last.discontinuity
              val projectNextRoadParts = project.reservedParts.filter(rp =>
                rp.roadNumber == road && rp.roadPartNumber > part)

              val nextProjectPart = projectNextRoadParts.filter(np => np.newLength.getOrElse(0L) > 0L && allProjectLinks.exists(l => l.roadPartNumber == np.roadPartNumber))
                .map(_.roadPartNumber).sorted.headOption
              val nextAddressPart = validRoadParts
                .filter(p => p > part).sorted
                .find(p => RoadAddressDAO.fetchByRoadPart(road, p, includeFloating = true)
                  .forall(ra => !allProjectLinks.exists(al => al.roadAddressId == ra.id && al.roadPartNumber != ra.roadPartNumber)))
              if (nextProjectPart.isEmpty && nextAddressPart.isEmpty && discontinuity != EndOfRoad) {
                error(project.id, ValidationErrorList.MissingEndOfRoad)(Seq(last))
              } else if (!(nextProjectPart.isEmpty && nextAddressPart.isEmpty) && discontinuity == EndOfRoad) {
                error(project.id, ValidationErrorList.EndOfRoadNotOnLastPart)(Seq(last))
              } else {
                None
              }
          } else {
            None
          }
        }
      }.toSeq
      val groupedErrors: Seq[ValidationErrorDetails] = afterCheckErrors.groupBy(_.validationError).map {
        g =>
          val ids: Seq[Long] = g._2.flatMap(_.affectedIds)
          val coords: Seq[ProjectCoordinates] = g._2.flatMap(_.coordinates)
          ValidationErrorDetails(g._2.head.projectId, g._1, ids, coords, None)
      }.toSeq
      groupedErrors ++ errors
    }

    def checkDiscontinuityOnLastPart(errors: Seq[ValidationErrorDetails]): Seq[ValidationErrorDetails] = {
      val allProjectLinks = ProjectDAO.getProjectLinks(project.id)
     val discontinuityErrors = seq.groupBy(_.roadNumber).flatMap { g =>
        val validRoadParts = RoadAddressDAO.getValidRoadParts(g._1.toInt, project.startDate)
       val trackIntervals = Seq(g._2.filter(_.track != RightSide), g._2.filter(_.track != LeftSide))
        trackIntervals.flatMap {
          interval =>
            val nonTerminated = interval.filter(r => r.status != LinkStatus.Terminated)
            if(nonTerminated.nonEmpty){
              val last = nonTerminated.maxBy(_.endAddrMValue)
              val (road, part) = (last.roadNumber, last.roadPartNumber)
              val discontinuity = last.discontinuity
              val projectNextRoadParts = project.reservedParts.filter(rp =>
                rp.roadNumber == road && rp.roadPartNumber > part)

              val nextProjectPart = projectNextRoadParts.filter(np => np.newLength.getOrElse(0L) > 0L && allProjectLinks.exists(l => l.roadPartNumber == np.roadPartNumber))
                .map(_.roadPartNumber).sorted.headOption
              val nextAddressPart = validRoadParts
                .filter(p => p > part).sorted
                .find(p => RoadAddressDAO.fetchByRoadPart(road, p, includeFloating = true)
                  .forall(ra => !allProjectLinks.exists(al => al.roadAddressId == ra.id && al.roadPartNumber != ra.roadPartNumber)))
              if (!(nextProjectPart.isEmpty && nextAddressPart.isEmpty)) {
                val nextLinks = getNextLinksFromParts(allProjectLinks, road, nextProjectPart, nextAddressPart)

                val rampDiscontinuity = if (isConnectingRoundabout(Seq(last)) && isRampValidation) {
                  discontinuity match {
                    case EndOfRoad | ChangingELYCode | Continuous =>
                      error(project.id, ValidationErrorList.RoadConnectingRoundabout,
                        s"Rampin ${last.roadNumber} tieosa ${last.roadPartNumber} päättyy kiertoliittymään. Korjaa lievä epäjatkuvuus")(Seq(last))
                    case _ => None
                  }
                } else None

                val isConnected = Seq(last).forall(lpl => nextLinks.exists(nl => Track.isTrackContinuous(nl.track, lpl.track) &&
                  connected(lpl, nl)))
               val normalDiscontinuity = discontinuity match {
                  case Continuous =>
                    if (!isConnected) error(project.id, ValidationErrorList.MajorDiscontinuityFound)(Seq(last)) else None
                  case MinorDiscontinuity | Discontinuous =>
                    if (isConnected) error(project.id, ValidationErrorList.ConnectedDiscontinuousLink)(Seq(last)) else None
                  case _ =>  None // no error, continue
                }
                rampDiscontinuity.orElse(normalDiscontinuity)
              } else None

          } else None
        }
      }.toSeq
      val groupedDiscontinuity: Seq[ValidationErrorDetails] = discontinuityErrors.groupBy(_.validationError).map {
        g =>
          val ids: Seq[Long] = g._2.flatMap(_.affectedIds)
          val coords: Seq[ProjectCoordinates] = g._2.flatMap(_.coordinates)
          ValidationErrorDetails(g._2.head.projectId, g._1, ids, coords, None)
      }.toSeq
      groupedDiscontinuity ++ errors
    }

    def checkEndOfRoadOutsideOfProject(errors: Seq[ValidationErrorDetails]): Seq[ValidationErrorDetails] = {
      endOfRoadOutSideProject.toSeq ++ errors
    }

    def endOfRoadOutSideProject: Option[ValidationErrorDetails] = {
      val (road, part): (Long, Long) = (seq.head.roadNumber, seq.head.roadPartNumber)
      val previousRoadPartNumber: Option[Long] = RoadAddressDAO.fetchPreviousRoadPartNumber(road, part)
      if (previousRoadPartNumber.nonEmpty) {
        val allProjectLinks = ProjectDAO.getProjectLinks(project.id)
        if (allProjectLinks.exists(link => link.roadNumber == road && link.roadPartNumber == previousRoadPartNumber.get))
          return None
        val previousRoadPartEnd: Option[RoadAddress] = RoadAddressDAO.fetchByRoadPart(road, previousRoadPartNumber.get, fetchOnlyEnd = true).headOption
        if (previousRoadPartEnd.nonEmpty) {
          if (previousRoadPartEnd.get.discontinuity == EndOfRoad)
            return outsideOfProjectError(project.id, alterMessage(ValidationErrorList.DoubleEndOfRoad, roadAndPart = Some(Seq((previousRoadPartEnd.get.roadNumber, previousRoadPartEnd.get.roadPartNumber)))))(List(previousRoadPartEnd.get))
        }
      }
      None
    }

    def getNextLinksFromParts(allProjectLinks: Seq[ProjectLink], road: Long, nextProjectPart: Option[Long], nextAddressPart: Option[Long]) = {
      if (nextProjectPart.nonEmpty && (nextAddressPart.isEmpty || nextProjectPart.get <= nextAddressPart.get))
        ProjectDAO.fetchByProjectRoadPart(road, nextProjectPart.get, project.id).filter(_.startAddrMValue == 0L)
      else {
        RoadAddressDAO.fetchByRoadPart(road, nextAddressPart.get, includeFloating = true)
          .filterNot(rp => allProjectLinks.exists(link => rp.roadPartNumber != link.roadPartNumber && rp.id == link.roadAddressId)).filter(_.startAddrMValue == 0L)
      }
    }

    def getLastValidLinkForParts = {
      seq.filter(r => r.status != LinkStatus.Terminated && r.endAddrMValue == seq.maxBy(_.endAddrMValue).endAddrMValue)
    }

    val continuityValidations: Seq[Seq[ValidationErrorDetails] =>  Seq[ValidationErrorDetails]] = Seq(
      checkContinuityBetweenParts,
      checkMinorDiscontinuityBetweenLinksOnPart,
      checkDiscontinuityBetweenLinksOnRamps,
      checkEndOfRoadOnLastPart,
      checkDiscontinuityOnLastPart,
      checkEndOfRoadOutsideOfProject
    )

    val continuityErrors :Seq[ValidationErrorDetails] = continuityValidations.foldLeft(Seq.empty[ValidationErrorDetails]) { case (errors, validation) =>
      validation(errors)
    }
    continuityErrors.distinct
  }

  def checkTrackCode(project: RoadAddressProject, projectLinks: Seq[ProjectLink]): Seq[ValidationErrorDetails] = {

    val notCombinedLinks = projectLinks.filterNot(_.track == Track.Combined)

    def isSameTrack(previous: ProjectLink, currentLink: ProjectLink): Boolean = {
      previous.track == currentLink.track && previous.endAddrMValue == currentLink.startAddrMValue
    }

    def getTrackInterval(links: Seq[ProjectLink], track: Track): Seq[ProjectLink] = {
      links.foldLeft(Seq.empty[ProjectLink]) { (linkSameTrack, current) => {
        if (current.track == track && (linkSameTrack.isEmpty || isSameTrack(linkSameTrack.last, current))) {
          linkSameTrack :+ current
        } else {
          linkSameTrack
        }
      }
      }.sortBy(_.startAddrMValue)
    }

    def checkMinMaxTrack(trackInterval: Seq[ProjectLink]): Option[ProjectLink] = {
      if (trackInterval.head.track != Combined) {
        val minTrackLink = trackInterval.minBy(_.startAddrMValue)
        val maxTrackLink = trackInterval.maxBy(_.endAddrMValue)
        val notCombinedLinksInRoadPart = notCombinedLinks.filter(l => l.roadNumber == minTrackLink.roadNumber && l.roadPartNumber == minTrackLink.roadPartNumber)
        if (!notCombinedLinksInRoadPart.exists(l => l.startAddrMValue == minTrackLink.startAddrMValue && l.track != minTrackLink.track)) {
          Some(minTrackLink)
        }
        else if (!notCombinedLinksInRoadPart.exists(l => l.endAddrMValue == maxTrackLink.endAddrMValue && l.track != maxTrackLink.track)) {
          Some(maxTrackLink)
        } else None
      } else None
    }

    def validateTrackTopology(trackInterval: Seq[ProjectLink]): Seq[ProjectLink] = {
      val validTrackInterval = trackInterval.filterNot(_.status == Terminated)
      if (validTrackInterval.nonEmpty) {
        checkMinMaxTrack(validTrackInterval) match {
          case Some(link) => Seq(link)
          case None => if (validTrackInterval.size > 1) {
            validTrackInterval.sliding(2).map{case Seq(first, second) => {
              if (first.endAddrMValue != second.startAddrMValue && first.id != second.id) {
                Some(first)
              } else None
            }}.toSeq.flatten
          } else Seq.empty[ProjectLink]
        }
      } else Seq.empty[ProjectLink]
    }

    def recursiveCheckTrackChange(links: Seq[ProjectLink], errorLinks: Seq[ProjectLink] = Seq()): Option[ValidationErrorDetails] = {
      if (links.isEmpty) {
        error(project.id, ValidationErrorList.InsufficientTrackCoverage)(errorLinks)
      } else {
        val trackToCheck = links.head.track
        val trackInterval = getTrackInterval(links.sortBy(o => (o.roadNumber, o.roadPartNumber, o.track.value, o.startAddrMValue)), trackToCheck)
        recursiveCheckTrackChange(links.filterNot(l => trackInterval.exists(lt => lt.id == l.id)),
          errorLinks ++ validateTrackTopology(trackInterval))
      }
    }

    val groupedLinks = notCombinedLinks.filterNot(_.status == LinkStatus.Terminated).groupBy(pl => (pl.roadNumber, pl.roadPartNumber))
    groupedLinks.map(roadPart => {
      recursiveCheckTrackChange(roadPart._2) match {
        case Some(errors) => Seq(errors)
        case _ => Seq()
      }
    }).headOption.getOrElse(Seq())
  }

  def checkRemovedEndOfRoadParts(project: RoadAddressProject): Seq[ValidationErrorDetails] = {
    // Pick only parts that have no length anymore and had end of road given before
    val terminatedParts = project.reservedParts.filter(rrp => rrp.addressLength.nonEmpty && rrp.newLength.getOrElse(0L) == 0L)
    val (endOfRoadParts, otherParts) = terminatedParts.partition(_.discontinuity.contains(EndOfRoad))

    // There is no part after or previous this part in project
    endOfRoadParts.filterNot(
      rrp => {
        val validRoadParts = RoadAddressDAO.getValidRoadParts(rrp.roadNumber).filter(v => v < rrp.roadPartNumber)
        project.reservedParts.exists(np => np.roadNumber == rrp.roadNumber &&
          (np.roadPartNumber > rrp.roadPartNumber || Some(np.roadPartNumber).contains(if (validRoadParts.nonEmpty) validRoadParts.last else None)) && np.newLength.getOrElse(0L) > 0L)
      })

      // There is no part that is not in this project that comes after this part (rrp)
      .filterNot(rrp => RoadAddressDAO.getValidRoadParts(rrp.roadNumber).exists(l => l > rrp.roadPartNumber &&
      !project.reservedParts.exists(p => p.roadNumber == rrp.roadNumber && p.roadPartNumber == l)))
      .flatMap { rrp =>
        val validRoadPartNumbers = RoadAddressDAO.getValidRoadParts(rrp.roadNumber).filter(v => v < rrp.roadPartNumber).filterNot(v => otherParts.map(_.roadPartNumber).contains(v))
        validRoadPartNumbers.lastOption.map { roadPartNumber =>
          val validLinks = RoadAddressDAO.fetchByRoadPart(rrp.roadNumber, roadPartNumber, fetchOnlyEnd = true)
          ValidationErrorDetails(project.id, alterMessage(ValidationErrorList.TerminationContinuity, roadAndPart = Some(Seq((rrp.roadNumber, roadPartNumber)))),
            Seq(validLinks.head.id),
            Seq(ProjectCoordinates(validLinks.head.geometry.head.x, validLinks.head.geometry.head.y, 12)), Some(""))
        }
      }
  }

  def checkProjectElyCodes(project: RoadAddressProject, projectLinks: Seq[ProjectLink]): Seq[ValidationErrorDetails] = {

    /**
      * Check the project links edges for adjacent road addresses that must have a different ely then a adjacent one
      *
      * @param project             - the current project
      * @param groupedProjectLinks - project links, grouped by road number and road part number
      * @return Validation Errors
      */
    def firstElyBorderCheck(project: RoadAddressProject, groupedProjectLinks: Map[(Long, Long), Seq[ProjectLink]]) = {
      /**
        * Method that will prepare the output of the validation error.
        *
        * @param validationError the validation error
        * @param pl              Sequence of the erroneous ProjectLinks
        * @return An optional value with eventual Validation error details
        */
      def error(validationError: ValidationError)(pl: Seq[BaseRoadAddress]): Option[ValidationErrorDetails] = {
        val (linkIds, points) = pl.map(pl => (pl.id, GeometryUtils.midPointGeometry(pl.geometry))).unzip
        if (linkIds.nonEmpty)
          Some(ValidationErrorDetails(project.id, validationError, linkIds.distinct,
            points.map(p => ProjectCoordinates(p.x, p.y, 12)).distinct, None))
        else
          None
      }

      groupedProjectLinks.flatMap(group => {
        val projectLinks = group._2.filter(_.discontinuity == Discontinuity.ChangingELYCode)
        val problemRoads = if (projectLinks.nonEmpty) {
          val (startRoad, endRoad) = if (projectLinks.size == 1) {
            (projectLinks.head, projectLinks.head)
          } else {
            (projectLinks.head, projectLinks.last)
          }

          val roadsValidation = evaluateBorderCheck(startRoad, endRoad, secondCheck = false)
          roadsValidation.filterNot(_.isEmpty).getOrElse(Seq())
        } else {
          Seq.empty[BaseRoadAddress]
        }
        val uniqueProblemRoads = problemRoads.groupBy(_.id).map(_._2.head).toSeq
        error(ValidationErrorList.RoadNotEndingInElyBorder)(uniqueProblemRoads)
      }).toSeq
    }

    /**
      * Check the adjacent road addresses of the edges of the project links for ely codes that are different to the ones in the project links,
      * if they are not, issue a error
      *
      * @param project             - the current project
      * @param groupedProjectLinks - project links, grouped by road number and road part number
      * @return Validation Errors
      */
    def secondElyBorderCheck(project: RoadAddressProject, groupedProjectLinks: Map[(Long, Long), Seq[ProjectLink]]) = {
      /**
        * Method that will prepare the output of the validation error.
        *
        * @param validationError the validation error
        * @param pl              Sequence of the erroneous ProjectLinks
        * @return An optional value with eventual Validation error details
        */
      def error(validationError: ValidationError)(pl: Seq[BaseRoadAddress]): Option[ValidationErrorDetails] = {
        val grouppedByDiscontinuity = pl.groupBy(_.discontinuity)
        val (gLinkIds, gPoints, gDiscontinuity) = grouppedByDiscontinuity.flatMap(g => {
          val links = g._2
          val zoomPoint = GeometryUtils.midPointGeometry(links.minBy(_.endAddrMValue).geometry)
          links.map(l => (l.id, zoomPoint, l.discontinuity))
        }).unzip3

        if (gLinkIds.nonEmpty) {
          if (gDiscontinuity.nonEmpty) {
            Some(ValidationErrorDetails(project.id, alterMessage(validationError, Option.empty, Option.empty, Option(gDiscontinuity.toSeq)), gLinkIds.toSeq.distinct,
              gPoints.map(p => ProjectCoordinates(p.x, p.y, 12)).toSeq.distinct, Option.empty[String]))
          } else Some(ValidationErrorDetails(project.id, validationError, gLinkIds.toSeq.distinct,
            gPoints.map(p => ProjectCoordinates(p.x, p.y, 12)).toSeq.distinct, Option.empty[String]))
        }
        else
          Option.empty[ValidationErrorDetails]
      }

      val validationProblems = groupedProjectLinks.flatMap(group => {
        val projectLinks = group._2
        val problemRoads = if (projectLinks.nonEmpty) {
          val (startRoad, endRoad) = if (projectLinks.size == 1) {
            (projectLinks.head, projectLinks.head)
          } else {
            (projectLinks.head, projectLinks.last)
          }
          val validationResult = if (startRoad.discontinuity.value != Discontinuity.ChangingELYCode.value) evaluateBorderCheck(startRoad, endRoad, secondCheck = true) else Option.empty[Seq[ProjectLink]]
          validationResult.filterNot(_.isEmpty).getOrElse(Seq())

        } else {
          Seq.empty[BaseRoadAddress]
        }
        val uniqueProblemRoads = problemRoads.groupBy(_.id).map(_._2.head).toSeq
        error(ValidationErrorList.RoadContinuesInAnotherEly)(uniqueProblemRoads)

      })
      validationProblems.toSeq
    }

    /**
      * Main validation we create a bounding box and search for adjacent road addresses to the edgeRoad.
      * Then check if the ely code changed between them, depending whether we are validation the firstBorderCheck or the
      * second we output the edgeRoad based on the finding (or not) of a road address with a different ely code then that of the edgeRoad.
      *
      * @param startRoad   - start of a road number/road part number project link
      * @param endRoad     - end of a road number/road part number project link
      * @param secondCheck - indicates what kind of search we use
      * @return an optional symbolizing a found invalid edgeRoad, or nothing.
      */
    def evaluateBorderCheck(startRoad: ProjectLink, endRoad: ProjectLink, secondCheck: Boolean): Option[Seq[ProjectLink]] = {
      /**
        * Helper method, will find ALL the road addresses in a bounding box whose center is the edge road
        *
        * @param headRoad A project link, at the start of it (lowest endAddressMValue)
        * @param tailRoad A project link, at the end of it(highest endAddressMValue)
        * @return Road addresses contained in a small bounding box
        */
      def findRoads(headRoad: ProjectLink, tailRoad: ProjectLink) = {
        val sp = startPoint(headRoad)
        val ep = endPoint(tailRoad)
        val connectingAtStart = {
          val lowerCorner = Point(sp.x - distanceToPoint, sp.y - distanceToPoint, sp.z - distanceToPoint)
          val higherCorner = Point(sp.x + distanceToPoint, sp.y + distanceToPoint, sp.z + distanceToPoint)
          val box = BoundingRectangle(lowerCorner, higherCorner)
          RoadAddressDAO.fetchRoadAddressesByBoundingBox(box, fetchOnlyFloating = false)
        }
        val connectingAtEnd = {
          val lowerCorner = Point(ep.x - distanceToPoint, ep.y - distanceToPoint, ep.z - distanceToPoint)
          val higherCorner = Point(ep.x + distanceToPoint, ep.y + distanceToPoint, ep.z + distanceToPoint)
          val box = BoundingRectangle(lowerCorner, higherCorner)
          RoadAddressDAO.fetchRoadAddressesByBoundingBox(box, fetchOnlyFloating = false)
        }
        connectingAtStart ++ connectingAtEnd
      }

      val roadAddresses = findRoads(startRoad, endRoad)
      if (roadAddresses.nonEmpty) {
        val filtered = roadAddresses.filterNot(ra => ra.roadNumber == startRoad.roadNumber && ra.roadPartNumber == startRoad.roadPartNumber &&
          !GeometryUtils.areAdjacent(ra.geometry, startRoad.geometry) && !GeometryUtils.areAdjacent(ra.geometry, endRoad.geometry))
        val diffEly = filtered.find(_.ely != startRoad.ely)
        if (!secondCheck && diffEly.isEmpty) {
          Option(Seq(endRoad))
        } else if (secondCheck && diffEly.isDefined) {
          Option(Seq(endRoad))
        } else Option.empty[Seq[ProjectLink]]
      } else Option.empty[Seq[ProjectLink]]
    }

    val workedProjectLinks = projectLinks.filterNot(_.status == LinkStatus.NotHandled)
    if (workedProjectLinks.nonEmpty) {
      val grouped = workedProjectLinks.groupBy(pl => (pl.roadNumber, pl.roadPartNumber)).map(group => group._1 -> group._2.sortBy(_.endAddrMValue))
      val projectLinksDiscontinuity = workedProjectLinks.map(_.discontinuity.value).distinct.toList
      if (projectLinksDiscontinuity.contains(Discontinuity.ChangingELYCode.value))
        firstElyBorderCheck(project, grouped)
      else
        secondElyBorderCheck(project, grouped)
    } else Seq.empty[ValidationErrorDetails]
  }

  private def alterMessage(validationError: ValidationError, elyBorderData: Option[Seq[Long]] = Option.empty[Seq[Long]],
                           roadAndPart: Option[Seq[(Long, Long)]] = Option.empty[Seq[(Long, Long)]],
                           discontinuity: Option[Seq[Discontinuity]] = Option.empty[Seq[Discontinuity]], projectDate: Option[String] = Option.empty[String]) = {
    val formattedMessage =
      if (projectDate.nonEmpty && roadAndPart.nonEmpty) {
        val unzippedRoadAndPart = roadAndPart.get.unzip
        val changedMsg = validationError.message.format(unzippedRoadAndPart._1.head, unzippedRoadAndPart._2.head, projectDate.get)
        changedMsg
      } else {
        validationError.message.format(if (elyBorderData.nonEmpty) {
          elyBorderData.get.toSet.mkString(", ")
        } else if (roadAndPart.nonEmpty) {
          roadAndPart.get.toSet.mkString(", ")
        } else if (discontinuity.nonEmpty) {
          discontinuity.get.groupBy(_.value).map(_._2.head.toString).mkString(", ")
        }
        else {
          validationError.message
        })
      }

    case object formattedMessageObject extends ValidationError {
      def value: Int = validationError.value

      def message: String = formattedMessage

      def notification: Boolean = validationError.notification
    }
    formattedMessageObject
  }
}

class ProjectValidationException(s: String) extends RuntimeException {
  override def getMessage: String = s
}

class NameExistsException(s: String) extends RuntimeException {
  override def getMessage: String = s
}
