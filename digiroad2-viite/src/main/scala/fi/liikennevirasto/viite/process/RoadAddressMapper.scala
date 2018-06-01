package fi.liikennevirasto.viite.process

import fi.liikennevirasto.digiroad2.{GeometryUtils, Point}
import fi.liikennevirasto.digiroad2.asset.SideCode
import fi.liikennevirasto.digiroad2.client.vvh.VVHClient
import fi.liikennevirasto.viite.dao.{CalibrationPoint, RoadAddress}
import fi.liikennevirasto.viite._

trait RoadAddressMapper {

  def mapRoadAddresses(roadAddressMapping: Seq[RoadAddressMapping])(ra: RoadAddress): Seq[RoadAddress] = {
    def truncate(geometry: Seq[Point], d1: Double, d2: Double) = {
      // When operating with fake geometries (automatic change tables) the geometry may not have correct length
      val startM = Math.min(Math.max(Math.min(d1, d2), 0.0), GeometryUtils.geometryLength(geometry))
      val endM = Math.min(Math.max(d1, d2), GeometryUtils.geometryLength(geometry))
      GeometryUtils.truncateGeometry3D(geometry, startM, endM)
    }

    // When mapping contains a larger span (sourceStart, sourceEnd) than the road address then split the mapping
    def adjust(mapping: RoadAddressMapping, startM: Double, endM: Double) = {
      if (withinTolerance(mapping.sourceStartM, startM) && withinTolerance(mapping.sourceEndM, endM))
        mapping
      else {
        val (newStartM, newEndM) =
          (if (withinTolerance(startM, mapping.sourceStartM) || startM < mapping.sourceStartM) mapping.sourceStartM else startM,
            if (withinTolerance(endM, mapping.sourceEndM) || endM > mapping.sourceEndM) mapping.sourceEndM else endM)
        val (newTargetStartM, newTargetEndM) = (mapping.interpolate(newStartM), mapping.interpolate(newEndM))
        val geomStartM = Math.min(mapping.sourceStartM, mapping.sourceEndM)
        val geomTargetStartM = Math.min(mapping.interpolate(mapping.sourceStartM), mapping.interpolate(mapping.sourceEndM))
        mapping.copy(sourceStartM = newStartM, sourceEndM = newEndM, sourceGeom =
          truncate(mapping.sourceGeom, newStartM - geomStartM, newEndM - geomStartM),
          targetStartM = newTargetStartM, targetEndM = newTargetEndM, targetGeom =
            truncate(mapping.targetGeom, newTargetStartM - geomTargetStartM, newTargetEndM - geomTargetStartM))
      }
    }

    roadAddressMapping.filter(_.matches(ra)).map(m => adjust(m, ra.startMValue, ra.endMValue)).map(adjMap => {
      val (sideCode, mappedGeom, (mappedStartAddrM, mappedEndAddrM)) =
        if (isDirectionMatch(adjMap))
          (ra.sideCode, adjMap.targetGeom, splitRoadAddressValues(ra, adjMap))
        else {
          (switchSideCode(ra.sideCode), adjMap.targetGeom.reverse,
            splitRoadAddressValues(ra, adjMap))
        }
      val (startM, endM) = (Math.min(adjMap.targetEndM, adjMap.targetStartM), Math.max(adjMap.targetEndM, adjMap.targetStartM))

      val startCP = ra.startCalibrationPoint match {
        case None => None
        case Some(cp) => if (cp.addressMValue == mappedStartAddrM) Some(cp.copy(linkId = adjMap.targetLinkId,
          segmentMValue = if (sideCode == SideCode.AgainstDigitizing) endM - startM else 0.0)) else None
      }
      val endCP = ra.endCalibrationPoint match {
        case None => None
        case Some(cp) => if (cp.addressMValue == mappedEndAddrM) Some(cp.copy(linkId = adjMap.targetLinkId,
          segmentMValue = if (sideCode == SideCode.TowardsDigitizing) endM - startM else 0.0)) else None
      }
      ra.copy(id = NewRoadAddress, startAddrMValue = startCP.map(_.addressMValue).getOrElse(mappedStartAddrM),
        endAddrMValue = endCP.map(_.addressMValue).getOrElse(mappedEndAddrM), linkId = adjMap.targetLinkId,
        startMValue = startM, endMValue = endM, sideCode = sideCode, adjustedTimestamp = VVHClient.createVVHTimeStamp(),
        calibrationPoints = (startCP, endCP), floating = false, geometry = mappedGeom)
    })
  }

  /** Used when road address span is larger than mapping: road address must be split into smaller parts
    *
    * @param roadAddress Road address to split
    * @param mapping Mapping entry that may or may not have smaller or larger span than road address
    * @return A pair of address start and address end values this mapping and road address applies to
    */
  private def splitRoadAddressValues(roadAddress: RoadAddress, mapping: RoadAddressMapping): (Long, Long) = {
    if (withinTolerance(roadAddress.startMValue, mapping.sourceStartM) && withinTolerance(roadAddress.endMValue, mapping.sourceEndM))
      (roadAddress.startAddrMValue, roadAddress.endAddrMValue)
    else {
      val (startM, endM) = GeometryUtils.overlap((roadAddress.startMValue, roadAddress.endMValue),(mapping.sourceStartM, mapping.sourceEndM)).get
      roadAddress.addressBetween(startM, endM)
    }
  }

  def postTransferChecks(seq: Seq[RoadAddress], source: Seq[RoadAddress]): Unit = {
    val (addrMin, addrMax) = (source.map(_.startAddrMValue).min, source.map(_.endAddrMValue).max)
    commonPostTransferChecks(seq, addrMin, addrMax)
  }

  def postTransferChecks(s: (RoadAddressSection, Seq[RoadAddress], Seq[RoadAddress])): Unit = {
    val (section, current, history) = s
    if(current.nonEmpty){
      if (current.groupBy(_.linkId).exists{ case (_, addresses) =>
        partition(addresses).size > 1})
      throw new InvalidAddressDataException(s"Address gaps generated for links ${current.groupBy(_.linkId).filter{ case (_, addresses) =>
        partition(addresses).size > 1}.keySet.mkString(", ")}")
    commonPostTransferChecks(current, section.startMAddr, section.endMAddr)
    }

    if(history.nonEmpty){
      if (history.groupBy(_.linkId).exists{ case (_, addresses) =>
        partition(addresses).size > 1})
        throw new InvalidAddressDataException(s"Address gaps generated for links ${history.groupBy(_.linkId).filter{ case (_, addresses) =>
          partition(addresses).size > 1}.keySet.mkString(", ")}")
      commonPostTransferChecks(history, section.startMAddr, section.endMAddr)
    }

  }

  def postTransferChecksForCurrent(s: (RoadAddressSection, Seq[LinkRoadAddressHistory])): Unit = {
    postTransferChecks((s._1, s._2.flatMap(_.currentSegments), Seq()))
  }

  def postTransferChecksForHistory(s: (RoadAddressSection, Seq[LinkRoadAddressHistory])): Unit = {
    postTransferChecks((s._1, Seq(), s._2.flatMap(_.historySegments)))
  }

  protected def commonPostTransferChecks(seq: Seq[RoadAddress], addrMin: Long, addrMax: Long): Unit = {
    calibrationPointCountCheck(false, seq)
    seq.find(_.startCalibrationPoint.nonEmpty) match {
      case Some(addr) => startCalibrationPointCheck(addr, addr.startCalibrationPoint.get, seq)
      case _ =>
    }
    seq.find(_.endCalibrationPoint.nonEmpty) match {
      case Some(addr) => endCalibrationPointCheck(addr, addr.endCalibrationPoint.get, seq)
      case _ =>
    }
    checkSingleSideCodeForLink(false, seq.groupBy(_.linkId))
    if (!seq.exists(_.startAddrMValue == addrMin))
      throw new InvalidAddressDataException(s"Generated address list does not start at $addrMin but ${seq.map(_.startAddrMValue).min}")
    if (!seq.exists(_.endAddrMValue == addrMax))
      throw new InvalidAddressDataException(s"Generated address list does not end at $addrMax but ${seq.map(_.endAddrMValue).max}")
    if (!seq.forall(ra => ra.startAddrMValue == addrMin || seq.exists(_.endAddrMValue == ra.startAddrMValue)))
      throw new InvalidAddressDataException(s"Generated address list was non-continuous")
    if (!seq.forall(ra => ra.endAddrMValue == addrMax || seq.exists(_.startAddrMValue == ra.endAddrMValue)))
      throw new InvalidAddressDataException(s"Generated address list was non-continuous")

  }

  def preTransferChecks(seq: Seq[RoadAddress]): Unit = {
    val nonHistoric = seq.filter(_.endDate.isEmpty)
    calibrationPointCountCheck(true, seq)
    nonHistoric.find(_.startCalibrationPoint.nonEmpty) match {
      case Some(addr) => startCalibrationPointCheck(addr, addr.startCalibrationPoint.get, seq)
      case _ =>
    }
    nonHistoric.find(_.endCalibrationPoint.nonEmpty) match {
      case Some(addr) => endCalibrationPointCheck(addr, addr.endCalibrationPoint.get, seq)
      case _ =>
    }
    checkSingleSideCodeForLink(false, nonHistoric.groupBy(_.linkId))
    val tracks = seq.map(_.track).toSet
    if (tracks.size > 1)
      throw new IllegalArgumentException(s"Multiple track codes found ${tracks.mkString(", ")}")
  }

  protected def startCalibrationPointCheck(addr: RoadAddress, cp: CalibrationPoint, seq: Seq[RoadAddress]): Unit = {
    if (addr.startAddrMValue != cp.addressMValue)
      throw new IllegalArgumentException(s"Start calibration point value mismatch in $cp")
    if (addr.sideCode == SideCode.TowardsDigitizing && Math.abs(cp.segmentMValue) > 0.0 ||
      addr.sideCode == SideCode.AgainstDigitizing && Math.abs(cp.segmentMValue - (addr.endMValue - addr.startMValue)) > MaxAllowedMValueError)
      throw new IllegalArgumentException(s"Start calibration point LRM mismatch in $cp")
  }

  protected def endCalibrationPointCheck(addr: RoadAddress, cp: CalibrationPoint, seq: Seq[RoadAddress]): Unit = {
    if (addr.endAddrMValue != cp.addressMValue)
      throw new IllegalArgumentException(s"End calibration point value mismatch in $cp")
    if (Math.abs(cp.segmentMValue -
      (addr.sideCode match {
        case SideCode.AgainstDigitizing => 0.0
        case SideCode.TowardsDigitizing => addr.endMValue - addr.startMValue
        case _ => Double.NegativeInfinity
      })
    ) > MinAllowedRoadAddressLength)
      throw new IllegalArgumentException(s"End calibration point LRM mismatch in $cp")
  }

  protected def calibrationPointCountCheck(before: Boolean, seq: Seq[RoadAddress]): Unit = {
    val str = if (before) "before" else "after"
    if (seq.count(_.startCalibrationPoint.nonEmpty) > 1)
      throw new InvalidAddressDataException(s"Too many starting calibration points $str transfer")
    if (seq.count(_.endCalibrationPoint.nonEmpty) > 1)
      throw new InvalidAddressDataException(s"Too many starting calibration points $str transfer")
  }

  protected def checkSingleSideCodeForLink(before: Boolean, grouped: Map[Long, Seq[RoadAddress]]): Unit = {
    val str = if (before) "found" else "generated"
    if (grouped.mapValues(_.groupBy(_.sideCode).keySet.size).exists{ case (_, sideCodes) => sideCodes > 1})
      throw new InvalidAddressDataException(s"Multiple side codes $str for links ${grouped.mapValues(_.groupBy(_.sideCode).keySet.size).filter(_._2 > 1).keySet.mkString(", ")}")

  }

  /**
    * Check if the sequence of points are going in matching direction (best matching)
    * This means that the starting and ending points are closer to each other than vice versa
    *
    * @param geom1 Geometry one
    * @param geom2 Geometry two
    */
  def isDirectionMatch(geom1: Seq[Point], geom2: Seq[Point]): Boolean = {
    val x = distancesBetweenEndPoints(geom1, geom2)
    x._1 < x._2
  }

  def isSideCodeChange(geom1: Seq[Point], geom2: Seq[Point]): Boolean = {
    GeometryUtils.areAdjacent(geom1.last, geom2.last) ||
      GeometryUtils.areAdjacent(geom1.head, geom2.head)
  }

  /**
    * Measure summed distance between two geometries: head-to-head + tail-to-head vs. head-to-tail + tail-to-head
 *
    * @param geom1 Geometry 1
    * @param geom2 Goemetry 2
    * @return h2h distance, h2t distance sums
    */
  def distancesBetweenEndPoints(geom1: Seq[Point], geom2: Seq[Point]) = {
    (geom1.head.distance2DTo(geom2.head) + geom1.last.distance2DTo(geom2.last),
      geom1.last.distance2DTo(geom2.head) + geom1.head.distance2DTo(geom2.last))
  }

  def minDistanceBetweenEndPoints(geom1: Seq[Point], geom2: Seq[Point]) = {
    val x = distancesBetweenEndPoints(geom1, geom2)
    Math.min(x._1, x._2)
  }

  def isDirectionMatch(r: RoadAddressMapping): Boolean = {
    ((r.sourceStartM - r.sourceEndM) * (r.targetStartM - r.targetEndM)) > 0
  }
  def withinTolerance(mValue1: Double, mValue2: Double) = {
    Math.abs(mValue1 - mValue2) < MinAllowedRoadAddressLength
  }

  /**
    * Partitioning for transfer checks. Stops at calibration points, changes of road part etc.
 *
    * @param roadAddresses
    * @return
    */
  protected def partition(roadAddresses: Iterable[RoadAddress]): Seq[RoadAddressSection] = {
    def combineTwo(r1: RoadAddress, r2: RoadAddress): Seq[RoadAddress] = {
      if (r1.endAddrMValue == r2.startAddrMValue && r1.endCalibrationPoint.isEmpty)
        Seq(r1.copy(discontinuity = r2.discontinuity, endAddrMValue = r2.endAddrMValue))
      else
        Seq(r2, r1)
    }
    def combine(roadAddressSeq: Seq[RoadAddress], result: Seq[RoadAddress] = Seq()): Seq[RoadAddress] = {
      if (roadAddressSeq.isEmpty)
        result.reverse
      else if (result.isEmpty)
        combine(roadAddressSeq.tail, Seq(roadAddressSeq.head))
      else
        combine(roadAddressSeq.tail, combineTwo(result.head, roadAddressSeq.head) ++ result.tail)
    }
    val grouped = roadAddresses.groupBy(ra => (ra.roadNumber, ra.roadPartNumber, ra.track, ra.commonHistoryId))
    grouped.mapValues(v => combine(v.toSeq.sortBy(_.startAddrMValue))).values.flatten.map(ra =>
      RoadAddressSection(ra.roadNumber, ra.roadPartNumber, ra.roadPartNumber,
        ra.track, ra.startAddrMValue, ra.endAddrMValue, ra.discontinuity, RoadType.Unknown, ra.ely, ra.reversed, ra.commonHistoryId)
    ).toSeq
  }

}
