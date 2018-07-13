package fi.liikennevirasto.viite.process.strategy

import fi.liikennevirasto.digiroad2.util.{Track}
import fi.liikennevirasto.viite.dao.CalibrationPointDAO.UserDefinedCalibrationPoint
import fi.liikennevirasto.viite.dao.{LinkStatus, ProjectLink}


class LinkStatusChangeTrackCalculatorStrategy extends TrackCalculatorStrategy {

  val AdjustmentToleranceMeters = 3L

  protected def getUntilLinkStatusChange(seq: Seq[ProjectLink], status: LinkStatus): (Seq[ProjectLink], Seq[ProjectLink]) = {
    val continuousProjectLinks = seq.takeWhile(pl => pl.status == status)
    (continuousProjectLinks, seq.drop(continuousProjectLinks.size))
  }

  override def applicableStrategy(headProjectLink: ProjectLink, projectLink: ProjectLink): Boolean = {
    //Will be applied if the link status changes FROM or TO a status equal "NEW" or "TERMINATED" and track is Left or Right
    projectLink.status != headProjectLink.status &&
      (projectLink.status == LinkStatus.New || headProjectLink.status == LinkStatus.New) &&
      (projectLink.track == Track.LeftSide || projectLink.track == Track.RightSide)
  }

  override def assignTrackMValues(startAddress: Option[Long], leftProjectLinks: Seq[ProjectLink], rightProjectLinks: Seq[ProjectLink], userDefinedCalibrationPoint: Map[Long, UserDefinedCalibrationPoint]): TrackCalculatorResult = {
    val (left, restLeft) = getUntilLinkStatusChange(leftProjectLinks, leftProjectLinks.head.status)
    val (right, restRight) = getUntilLinkStatusChange(rightProjectLinks, rightProjectLinks.head.status)

    val (lastLeft, lastRight) = (left.last, right.last)

    if (lastRight.endAddrMValue <= lastLeft.endAddrMValue) {
      val distance = lastRight.toMeters(lastLeft.endAddrMValue - lastRight.endAddrMValue)
      if (distance < AdjustmentToleranceMeters) {
        adjustTwoTracks(startAddress, left, right, userDefinedCalibrationPoint, restLeft, restRight)
      } else {
        val (newLeft, newRestLeft) = getUntilNearestAddress(leftProjectLinks, lastRight.endAddrMValue)
        adjustTwoTracks(startAddress, newLeft, right, userDefinedCalibrationPoint, newRestLeft, restRight)
      }
    } else {
      val distance = lastLeft.toMeters(lastRight.endAddrMValue - lastLeft.endAddrMValue)
      if (distance < AdjustmentToleranceMeters) {
        adjustTwoTracks(startAddress, left, right, userDefinedCalibrationPoint, restLeft, restRight)
      } else {
        val (newRight, newRestRight) = getUntilNearestAddress(rightProjectLinks, lastLeft.endAddrMValue)
        adjustTwoTracks(startAddress, left, newRight, userDefinedCalibrationPoint, restLeft, newRestRight)
      }
    }
  }
}


class TerminatedLinkStatusChangeStrategy extends  LinkStatusChangeTrackCalculatorStrategy {

  protected def adjustTwoTrackss(startAddress: Option[Long], endAddress: Option[Long], leftProjectLinks: Seq[ProjectLink], rightProjectLinks: Seq[ProjectLink], calibrationPoints: Map[Long, UserDefinedCalibrationPoint],
                                restLeftProjectLinks: Seq[ProjectLink] = Seq(), restRightProjectLinks: Seq[ProjectLink] = Seq()): TrackCalculatorResult = {

    val availableCalibrationPoint = calibrationPoints.get(rightProjectLinks.last.id).orElse(calibrationPoints.get(leftProjectLinks.last.id))

    val startSectionAddress = startAddress.getOrElse(getFixedAddress(leftProjectLinks.head, rightProjectLinks.head)._1)
    val estimatedEnd = endAddress.getOrElse(getFixedAddress(leftProjectLinks.last, rightProjectLinks.last, availableCalibrationPoint)._2)

    val (adjustedLeft, adjustedRight) = adjustTwoTracks(rightProjectLinks, leftProjectLinks, startSectionAddress, estimatedEnd, calibrationPoints)

    //The getFixedAddress method have to be call twice because when we do it the first time we are getting the estimated end measure, that will be used for the calculation of
    // NEW sections. For example if in one of the sides we have a TRANSFER section it will use the value after recalculate all the existing sections with the original length.
    val endSectionAddress = endAddress.getOrElse(getFixedAddress(adjustedLeft.last, adjustedRight.last, availableCalibrationPoint)._2)

    TrackCalculatorResult(setLastEndAddrMValue(adjustedLeft, endSectionAddress), setLastEndAddrMValue(adjustedRight, endSectionAddress), startSectionAddress, endSectionAddress, restLeftProjectLinks, restRightProjectLinks)
  }

  override def applicableStrategy(headProjectLink: ProjectLink, projectLink: ProjectLink): Boolean = {
    //Will be applied if the link status changes FROM or TO a status equal "TERMINATED" and track is Left or Right
    projectLink.status != headProjectLink.status &&
      (projectLink.status == LinkStatus.Terminated || headProjectLink.status == LinkStatus.Terminated) &&
      (projectLink.track == Track.LeftSide || projectLink.track == Track.RightSide)
  }

  override def assignTrackMValues(startAddress: Option[Long], leftProjectLinks: Seq[ProjectLink], rightProjectLinks: Seq[ProjectLink], userDefinedCalibrationPoint: Map[Long, UserDefinedCalibrationPoint]): TrackCalculatorResult = {
    val (left, restLeft) = getUntilLinkStatusChange(leftProjectLinks, leftProjectLinks.head.status)
    val (right, restRight) = getUntilLinkStatusChange(rightProjectLinks, rightProjectLinks.head.status)

    val (lastLeft, lastRight) = (left.last, right.last)

    if (lastRight.endAddrMValue <= lastLeft.endAddrMValue) {
      val (newLeft, newRestLeft) = getUntilNearestAddress(leftProjectLinks, lastRight.endAddrMValue)
      adjustTwoTrackss(startAddress, Some(lastRight.endAddrMValue), newLeft, right, userDefinedCalibrationPoint, newRestLeft, restRight)
    } else {
      val (newRight, newRestRight) = getUntilNearestAddress(rightProjectLinks, lastLeft.endAddrMValue)
      adjustTwoTrackss(startAddress, Some(lastLeft.endAddrMValue), left, newRight, userDefinedCalibrationPoint, restLeft, newRestRight)
    }
  }
}