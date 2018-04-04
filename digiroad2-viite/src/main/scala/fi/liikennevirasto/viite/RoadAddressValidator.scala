package fi.liikennevirasto.viite

import fi.liikennevirasto.digiroad2.GeometryUtils
import fi.liikennevirasto.viite.dao._
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

object RoadAddressValidator {

  def checkAvailable(number: Long, part: Long, currentProject: RoadAddressProject, linkStatus: LinkStatus, projectLinks: Seq[ProjectLink]): Unit = {
    def checkPartOriginal(originalAddresses: List[RoadAddress], filteredLinks: Seq[ProjectLink], fmt: DateTimeFormatter): Unit = {

      val areAdjacent = originalAddresses.exists(ora => {
        filteredLinks.exists(fl => {
          GeometryUtils.areAdjacent(ora.geometry, fl.geometry)
        })
      })
      val areOverlapping = originalAddresses.exists(ra => {
        filteredLinks.exists(fl => {
          GeometryUtils.overlaps((ra.startMValue, ra.endMValue), (fl.startMValue, fl.endMValue))
        })
      })
      //Fetch all used projectId's from
      val allProjectsId = RoadAddressDAO.fetchProjectIdsOfReservedRoads(number, part)
      //
      val moreUses = if (allProjectsId.isEmpty) {
        true
      } else {
        val removed = allProjectsId.filterNot(api => {
          filteredLinks.map(_.projectId).contains(api)
        })
        removed.nonEmpty
      }
      if ((!areAdjacent || !areOverlapping) && moreUses) {
        throw new ProjectValidationException(RoadNotAvailableMessage.format(number, part, currentProject.startDate.toString(fmt)))
      }
    }

    if (linkStatus.value != LinkStatus.New.value) {
      if (RoadAddressDAO.isNotAvailableForProject(number, part, currentProject.id)) {
        val fmt = DateTimeFormat.forPattern("dd.MM.yyyy")
        throw new ProjectValidationException(RoadNotAvailableMessage.format(number, part, currentProject.startDate.toString(fmt)))
      }
    } else {
      if (RoadAddressDAO.isNotAvailableForProjectNew(number, part, currentProject.id)) {
        val fmt = DateTimeFormat.forPattern("dd.MM.yyyy")
        //We need to determine if the projectLinks are indeed adjacent to the original RoadAddress
        val filteredLinks = projectLinks.filter(p => {
          p.roadNumber == number && p.roadPartNumber == part && p.status.value == linkStatus.value
        }).sortBy(_.endAddrMValue)
        val originalAddresses = RoadAddressDAO.fetchByRoadPart(number, part)
        val isAPartOfOriginals = originalAddresses.exists(oR => {
          filteredLinks.exists(_.linkId == oR.linkId)
        })
        //Are adjacent or there is an overlapping road address with the project link
        if (!isAPartOfOriginals)
          throw new ProjectValidationException(RoadNotAvailableMessage.format(number, part, currentProject.startDate.toString(fmt)))
        else checkPartOriginal(originalAddresses, filteredLinks, fmt)
      }
    }
  }

  def checkNotReserved(number: Long, part: Long, currentProject: RoadAddressProject): Unit = {
    val project = ProjectDAO.roadPartReservedByProject(number, part, currentProject.id, withProjectId = true)
    if (project.nonEmpty) {
      throw new ProjectValidationException(s"TIE $number OSA $part on jo varattuna projektissa ${project.get}, tarkista tiedot")
    }
  }

  def checkProjectExists(id: Long): Unit = {
    if (ProjectDAO.getRoadAddressProjectById(id).isEmpty)
      throw new ProjectValidationException("Projektikoodilla ei löytynyt projektia")
  }

}
