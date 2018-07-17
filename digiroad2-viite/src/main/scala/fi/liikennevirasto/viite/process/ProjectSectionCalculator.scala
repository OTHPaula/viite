package fi.liikennevirasto.viite.process

import fi.liikennevirasto.digiroad2.asset.SideCode
import fi.liikennevirasto.digiroad2.asset.SideCode.AgainstDigitizing
import fi.liikennevirasto.digiroad2.linearasset.RoadLink
import fi.liikennevirasto.digiroad2.util.{RoadAddressException, Track}
import fi.liikennevirasto.digiroad2.{GeometryUtils, Point}
import fi.liikennevirasto.viite.{RampsMaxBound, RampsMinBound, RoadType}
import fi.liikennevirasto.viite.dao.CalibrationPointDAO.UserDefinedCalibrationPoint
import fi.liikennevirasto.viite.dao._
import fi.liikennevirasto.viite.process.strategy.{RoadAddressSectionCalculatorContext, TrackCalculatorContext}
import org.slf4j.LoggerFactory


object ProjectSectionCalculator {

  private val logger = LoggerFactory.getLogger(getClass)

  /**
    * NOTE! Should be called from project service only at recalculate method - other places are usually wrong places
    * and may miss user given calibration points etc.
    * Recalculates the AddressMValues for project links. LinkStatus.New will get reassigned values and all
    * others will have the transfer/unchanged rules applied for them.
    * Terminated links will not be recalculated
    *
    * @param projectLinks List of addressed links in project
    * @return Sequence of project links with address values and calibration points.
    */
  def assignMValues(projectLinks: Seq[ProjectLink], userGivenCalibrationPoints: Seq[UserDefinedCalibrationPoint] = Seq()): Seq[ProjectLink] = {
    logger.info(s"Starting MValue assignment for ${projectLinks.size} links")
    val (terminated, others) = projectLinks.partition(_.status == LinkStatus.Terminated)
    val (newLinks, nonTerminatedLinks) = others.partition(l => l.status == LinkStatus.New)
    try {

      val calculator = RoadAddressSectionCalculatorContext.getStrategy(others)
      logger.info(s"${calculator.name} strategy")
      calculator.assignMValues(newLinks, nonTerminatedLinks, userGivenCalibrationPoints)
      //val recalculated = calculator.assignMValues(newLinks, nonTerminatedLinks, userGivenCalibrationPoints)

      //recalculated ++ terminated
      //recalculated ++ assignTerminatedAddressMeasures(terminated, recalculated)
//      val groups = groupTerminatedLinksRecursive(terminated, recalculated.filter(_.status != LinkStatus.New))
//      recalculated ++ groups.flatMap(_.terminated)
      //recalculated ++ calculateSectionAddressValues(terminated, recalculated.filter(_.status != LinkStatus.New))
      //recalculated ++ recalculate(terminated, recalculated)


    } finally {
      logger.info(s"Finished MValue assignment for ${projectLinks.size} links")
    }
  }


  def assignTerminatedMValues(terminated: Seq[ProjectLink], nonTerminatedLinks: Seq[ProjectLink]) : Seq[ProjectLink] = {
    logger.info(s"Starting MValue assignment for ${terminated.size} links")
    try{
      recalculate(terminated, nonTerminatedLinks)
    } finally {
      logger.info(s"Finished MValue assignment for ${terminated.size} links")
    }
  }

  case class TerminatedGroup(terminated: Seq[ProjectLink], recalculatedProjectLinks: Seq[ProjectLink], startAddrM: Long, endAddrM: Long)

  val MaxDistanceForConnectedLinks = 0.1

  //TODO should have in account the more when there is more than one connected link with a loop for example
  //TODO
  def findSection(projectLinks: Seq[ProjectLink], recalculatedProjectLinks: Seq[ProjectLink]) : (TerminatedGroup, Seq[ProjectLink]) = {

    def getAddressMeasureAtPoint(point: Point, projectLink: ProjectLink) : Long = {
      if(projectLink.sideCode == SideCode.TowardsDigitizing){
        if(GeometryUtils.areAdjacent(projectLink.geometry.head, point, MaxDistanceForConnectedLinks)) {
          projectLink.startAddrMValue
        } else {
          projectLink.endAddrMValue
        }
      } else {
        if(GeometryUtils.areAdjacent(projectLink.geometry.last, point, MaxDistanceForConnectedLinks)) {
          projectLink.startAddrMValue
        } else {
          projectLink.endAddrMValue
        }
      }
    }

    def getEndPoints(projectLink: ProjectLink) = {
      if(projectLink.sideCode == SideCode.TowardsDigitizing) Seq(projectLink.geometry.head, projectLink.geometry.last) else Seq(projectLink.geometry.last, projectLink.geometry.head)
    }

    def isConnectedAtPoint(point: Point, projectLink: ProjectLink): Boolean = {
      GeometryUtils.minimumDistance(point,
        GeometryUtils.geometryEndpoints(projectLink.geometry)) < MaxDistanceForConnectedLinks
    }

    def isConnectedAtEnd(pl1: ProjectLink, pl2: ProjectLink): Boolean = isConnectedAtPoint(getEndPoints(pl1).last, pl2)

    def isConnectedAtStart(pl1: ProjectLink, pl2: ProjectLink): Boolean = isConnectedAtPoint(getEndPoints(pl1).head, pl2)

    val continuousProjectLinks = projectLinks.zip(projectLinks.tail).takeWhile{case (current, next) =>
      isConnectedAtEnd(current, next) //&& !recalculatedProjectLinks.exists(pl => isConnectedAtEnd(current, pl))
    }.map(_._1)
    val pRest = projectLinks.drop(continuousProjectLinks.size)

    val(section, rest) = if (pRest.nonEmpty) (continuousProjectLinks :+ pRest.head, pRest.tail) else (continuousProjectLinks, pRest)

    val connectedAtStart = recalculatedProjectLinks.find(pl => isConnectedAtStart(section.head, pl))
    val startAddress = connectedAtStart.map(pl => getAddressMeasureAtPoint(getEndPoints(section.head).head, pl)).getOrElse(section.head.startAddrMValue)

    val result = ProjectSectionMValueCalculator.assignLinkValues(section, startAddress)

    //TODO Do not make sense to se the values at the end
//    val result = recalculatedProjectLinks.find(pl => isConnectedAtEnd(section.last, pl)) match {
//      case Some(connectedAtEnd) => recalculated.init :+ recalculated.last.copy(endAddrMValue = getAddressMeasureAtPoint(getEndPoints(section.last).last, connectedAtEnd))
//      case _ => recalculated
//    }

    (TerminatedGroup(result, recalculatedProjectLinks.filter(pl => pl.startAddrMValue >= startAddress && pl.endAddrMValue <= result.last.endAddrMValue), startAddress, result.last.endAddrMValue), rest)
  }

  def groupTerminatedLinksRecursive(projectLinks: Seq[ProjectLink], recalculatedProjectLinks: Seq[ProjectLink], acc: Seq[TerminatedGroup] = Seq.empty) : Seq[TerminatedGroup] = {
    projectLinks match {
      case Seq() => acc
      case _ =>
        val (section, rest) = findSection(projectLinks, recalculatedProjectLinks)
        groupTerminatedLinksRecursive(rest, recalculatedProjectLinks, acc :+ section)
    }
  }

  //TODO instead of having all of those ifs to return a empty sequence, we can have a try catch we have for the base recalculation
  def calculateSectionAddressValues(terminated: Seq[ProjectLink], recalculateProjectLinks: Seq[ProjectLink]) = {

    def getContinuousTrack(seq: Seq[ProjectLink]): (Seq[ProjectLink], Seq[ProjectLink]) = {
      val track = seq.headOption.map(_.track).getOrElse(Track.Unknown)
      val continuousProjectLinks = seq.takeWhile(pl => pl.track == track)
      (continuousProjectLinks, seq.drop(continuousProjectLinks.size))
    }

    def adjustTracksToMatch(leftLinks: Seq[ProjectLink], rightLinks: Seq[ProjectLink], previousStart: Option[Long]): (Seq[ProjectLink], Seq[ProjectLink]) = {
      if (rightLinks.isEmpty && leftLinks.isEmpty) {
        (Seq(), Seq())
      } else {
        val (firstRight, restRight) = getContinuousTrack(rightLinks)
        val (firstLeft, restLeft) = getContinuousTrack(leftLinks)

        if (firstRight.isEmpty || firstLeft.isEmpty)
          throw new RoadAddressException(s"Mismatching tracks, R ${firstRight.size}, L ${firstLeft.size}")

        val strategy = TrackCalculatorContext.getStrategy(firstLeft, firstRight)
        val trackCalcResult = strategy.assignTrackMValues(previousStart, firstLeft, firstRight, Map())

        val (adjustedRestRight, adjustedRestLeft) = adjustTracksToMatch(trackCalcResult.restLeft ++ restLeft, trackCalcResult.restRight ++ restRight, Some(trackCalcResult.endAddrMValue))

        val (adjustedLeft, adjustedRight) = strategy.setCalibrationPoints(trackCalcResult, Map())

        (adjustedLeft ++ adjustedRestRight, adjustedRight ++ adjustedRestLeft)
      }
    }

    if(terminated.isEmpty){
      Seq()
    } else {
      val leftSideProjectLinks = terminated.filter(_.track != Track.RightSide).sortBy(_.startAddrMValue)
      val rightSideProjectLinks = terminated.filter(_.track != Track.LeftSide).sortBy(_.startAddrMValue)

      val leftGroups = groupTerminatedLinksRecursive(leftSideProjectLinks, recalculateProjectLinks)
      val rightGroups = groupTerminatedLinksRecursive(rightSideProjectLinks, recalculateProjectLinks)

      if(leftGroups.flatMap(_.terminated).isEmpty || rightGroups.flatMap(_.terminated).isEmpty){
        Seq()
      } else {
        val (left, right) = adjustTracksToMatch(leftGroups.flatMap(_.terminated).sortBy(_.startAddrMValue), rightGroups.flatMap(_.terminated).sortBy(_.startAddrMValue), None)
        val calculatedSections = TrackSectionOrder.createCombinedSections(right, left)
        calculatedSections.flatMap { sec =>
          if (sec.right == sec.left)
            sec.right.links
          else {
            sec.right.links ++ sec.left.links
          }
        }
      }
    }
  }

  def recalculate(terminated: Seq[ProjectLink], recalculateProjectLinks: Seq[ProjectLink]) : Seq[ProjectLink] = {

    def fromProjectLinks(s: Seq[ProjectLink]): TrackSection = {
      val pl = s.head
      TrackSection(pl.roadNumber, pl.roadPartNumber, pl.roadAddressTrack.get, s.map(_.geometryLength).sum, s)
    }

    def groupIntoSections(seq: Seq[ProjectLink]): Seq[TrackSection] = {
      if (seq.isEmpty)
        throw new InvalidAddressDataException("Missing track")
      val changePoints = seq.zip(seq.tail).filter{ case (pl1, pl2) => pl1.roadAddressTrack.get != pl2.roadAddressTrack.get}
      seq.foldLeft(Seq(Seq[ProjectLink]())) { case (tracks, pl) =>
        if (changePoints.exists(_._2 == pl)) {
          Seq(Seq(pl)) ++ tracks
        } else {
          Seq(tracks.head ++ Seq(pl)) ++ tracks.tail
        }
      }.reverse.map(fromProjectLinks)
    }

    def process(part: (Long, Long), projectLinks: Seq[ProjectLink]): Seq[ProjectLink] = {
      try {
        if (terminated.isEmpty) {
          Seq[ProjectLink]()
        } else {

          val left = projectLinks.filter(pl => pl.roadAddressTrack.getOrElse(pl.track) != Track.RightSide).sortBy(_.roadAddressStartAddrM)
          val right = projectLinks.filter(pl => pl.roadAddressTrack.getOrElse(pl.track) != Track.LeftSide).sortBy(_.roadAddressStartAddrM)

          if (left.isEmpty || right.isEmpty) {
            Seq[ProjectLink]()
          } else {
            val leftLinks = ProjectSectionMValueCalculator.assignLinkValues(left, addrSt = 0)
            val rightLinks = ProjectSectionMValueCalculator.assignLinkValues(right, addrSt = 0)

            val (lefta, righta) = adjustTracksToMatch(leftLinks, rightLinks, None)
            val calculatedSections = TrackSectionOrder.createCombinedSectionss(groupIntoSections(righta), groupIntoSections(lefta))
            calculatedSections.flatMap { sec =>
              if (sec.right == sec.left)
                sec.right.links
              else {
                sec.right.links ++ sec.left.links
              }
            }.filter(_.status == LinkStatus.Terminated)
          }
        }
      } catch {
        case ex: InvalidAddressDataException =>
          logger.info(s"Can't calculate road/road part ${part._1}/${part._2}: " + ex.getMessage)
          terminated
        case ex: NoSuchElementException =>
          logger.info("Delta calculation failed: " + ex.getMessage, ex)
          terminated
        case ex: NullPointerException =>
          logger.info("Delta calculation failed (NPE)", ex)
          terminated
        case ex: Throwable =>
          logger.info("Delta calculation not possible: " + ex.getMessage)
          terminated
      }
    }

    def getContinuousTrack(seq: Seq[ProjectLink]): (Seq[ProjectLink], Seq[ProjectLink]) = {
      val track = seq.headOption.map(_.roadAddressTrack.get).getOrElse(Track.Unknown)
      val continuousProjectLinks = seq.takeWhile(pl => pl.roadAddressTrack.get == track)
      (continuousProjectLinks, seq.drop(continuousProjectLinks.size))
    }

    def adjustTracksToMatch(leftLinks: Seq[ProjectLink], rightLinks: Seq[ProjectLink], previousStart: Option[Long]): (Seq[ProjectLink], Seq[ProjectLink]) = {
      if (rightLinks.isEmpty && leftLinks.isEmpty) {
        (Seq(), Seq())
      } else {
        val (firstRight, restRight) = getContinuousTrack(rightLinks)
        val (firstLeft, restLeft) = getContinuousTrack(leftLinks)

        if (firstRight.isEmpty || firstLeft.isEmpty)
          throw new RoadAddressException(s"Mismatching tracks, R ${firstRight.size}, L ${firstLeft.size}")

        val strategy = TrackCalculatorContext.getStrategy(firstLeft, firstRight)
        val trackCalcResult = strategy.assignTrackMValues(previousStart, firstLeft, firstRight, Map())

        val (adjustedRestRight, adjustedRestLeft) = adjustTracksToMatch(trackCalcResult.restLeft ++ restLeft, trackCalcResult.restRight ++ restRight, Some(trackCalcResult.endAddrMValue))

        val (adjustedLeft, adjustedRight) = strategy.setCalibrationPoints(trackCalcResult, Map())

        (adjustedLeft ++ adjustedRestRight, adjustedRight ++ adjustedRestLeft)
      }
    }

    val allProjectLinks = recalculateProjectLinks.filter(_.status != LinkStatus.New) ++ terminated
    //TODO should return a excetion here and on the assing m values
    val group = allProjectLinks.groupBy(record => (record.roadAddressRoadNumber.getOrElse(record.roadNumber), record.roadAddressRoadPart.getOrElse(record.roadPartNumber)))

    group.flatMap { case (part, projectLinks) =>
      process(part, projectLinks)
    }.toSeq
  }
}

case class RoadAddressSection(roadNumber: Long, roadPartNumberStart: Long, roadPartNumberEnd: Long, track: Track,
                              startMAddr: Long, endMAddr: Long, discontinuity: Discontinuity, roadType: RoadType, ely: Long, reversed: Boolean, commonHistoryId: Long) {
  def includes(ra: BaseRoadAddress): Boolean = {
    // within the road number and parts included
    ra.roadNumber == roadNumber && ra.roadPartNumber >= roadPartNumberStart && ra.roadPartNumber <= roadPartNumberEnd &&
      // and on the same track
      ra.track == track &&
      // and by reversed direction
      ra.reversed == reversed &&
      // and not starting before this section start or after this section ends
      !(ra.startAddrMValue < startMAddr && ra.roadPartNumber == roadPartNumberStart ||
        ra.startAddrMValue > endMAddr && ra.roadPartNumber == roadPartNumberEnd) &&
      // and not ending after this section ends or before this section starts
      !(ra.endAddrMValue > endMAddr && ra.roadPartNumber == roadPartNumberEnd ||
        ra.endAddrMValue < startMAddr && ra.roadPartNumber == roadPartNumberStart) &&
      // and same common history
      ra.commonHistoryId == commonHistoryId
  }
}

case class RoadLinkLength(linkId: Long, geometryLength: Double)

case class TrackSection(roadNumber: Long, roadPartNumber: Long, track: Track,
                        geometryLength: Double, links: Seq[ProjectLink]) {
  def reverse = TrackSection(roadNumber, roadPartNumber, track, geometryLength,
    links.map(l => l.copy(sideCode = SideCode.switch(l.sideCode))).reverse)

  lazy val startGeometry: Point = links.head.sideCode match {
    case AgainstDigitizing => links.head.geometry.last
    case _ => links.head.geometry.head
  }
  lazy val endGeometry: Point = links.last.sideCode match {
    case AgainstDigitizing => links.last.geometry.head
    case _ => links.last.geometry.last
  }
  lazy val startAddrM: Long = links.map(_.startAddrMValue).min
  lazy val endAddrM: Long = links.map(_.endAddrMValue).max

  def toAddressValues(start: Long, end: Long): TrackSection = {
    val runningLength = links.scanLeft(0.0) { case (d, pl) => d + pl.geometryLength }
    val coeff = (end - start) / runningLength.last
    val updatedLinks = links.zip(runningLength.zip(runningLength.tail)).map { case (pl, (st, en)) =>
      pl.copy(startAddrMValue = Math.round(start + st * coeff), endAddrMValue = Math.round(start + en * coeff))
    }
    this.copy(links = updatedLinks)
  }
}

case class CombinedSection(startGeometry: Point, endGeometry: Point, geometryLength: Double, left: TrackSection, right: TrackSection) {
  lazy val sideCode: SideCode = {
    if (GeometryUtils.areAdjacent(startGeometry, right.links.head.geometry.head))
      right.links.head.sideCode
    else
      SideCode.apply(5 - right.links.head.sideCode.value)
  }

  lazy val addressStartGeometry: Point = sideCode match {
    case AgainstDigitizing => endGeometry
    case _ => startGeometry
  }

  lazy val addressEndGeometry: Point = sideCode match {
    case AgainstDigitizing => startGeometry
    case _ => endGeometry
  }

  lazy val linkStatus: LinkStatus = right.links.head.status

  lazy val startAddrM: Long = right.links.map(_.startAddrMValue).min

  lazy val endAddrM: Long = right.links.map(_.endAddrMValue).max

  lazy val linkStatusCodes: Set[LinkStatus] = (right.links.map(_.status) ++ left.links.map(_.status)).toSet
}

