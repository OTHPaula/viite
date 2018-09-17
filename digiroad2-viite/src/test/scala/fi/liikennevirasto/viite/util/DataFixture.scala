package fi.liikennevirasto.viite.util

import java.sql.Timestamp
import java.util.Properties

import com.googlecode.flyway.core.Flyway
import fi.liikennevirasto.digiroad2._
import fi.liikennevirasto.digiroad2.asset.LinkGeomSource
import fi.liikennevirasto.digiroad2.client.vvh.VVHClient
import fi.liikennevirasto.digiroad2.oracle.OracleDatabase
import fi.liikennevirasto.digiroad2.oracle.OracleDatabase.ds
import fi.liikennevirasto.digiroad2.service.RoadLinkService
import fi.liikennevirasto.digiroad2.util.{MunicipalityCodeImporter, SqlScriptRunner}
import fi.liikennevirasto.viite.AddressConsistencyValidator.AddressError.InconsistentLrmHistory
import fi.liikennevirasto.viite._
import fi.liikennevirasto.viite.dao.RoadNetworkDAO.getLatestRoadNetworkVersionId
import fi.liikennevirasto.viite.dao._
import fi.liikennevirasto.viite.process._
import fi.liikennevirasto.viite.util.AssetDataImporter.Conversion
import org.joda.time.format.PeriodFormatterBuilder
import org.joda.time.{DateTime, Period}

import scala.collection.parallel.immutable.ParSet
import scala.collection.parallel.ForkJoinTaskSupport
import scala.language.postfixOps

object DataFixture {
  val TestAssetId = 300000
  lazy val properties: Properties = {
    val props = new Properties()
    props.load(getClass.getResourceAsStream("/bonecp.properties"))
    props
  }
  lazy val dr2properties: Properties = {
    val props = new Properties()
    props.load(getClass.getResourceAsStream("/digiroad2.properties"))
    props
  }


  val dataImporter = new AssetDataImporter
  lazy val vvhClient: VVHClient = {
    new VVHClient(dr2properties.getProperty("digiroad2.VVHRestApiEndPoint"))
  }

  lazy val continuityChecker = new ContinuityChecker(new RoadLinkService(vvhClient, new DummyEventBus, new DummySerializer))

  private lazy val hms = new PeriodFormatterBuilder() minimumPrintedDigits(2) printZeroAlways() appendHours() appendSeparator(":") appendMinutes() appendSuffix(":") appendSeconds() toFormatter

  private lazy val geometryFrozen: Boolean = dr2properties.getProperty("digiroad2.VVHRoadlink.frozen", "false").toBoolean

  private lazy val numberThreads: Int = 2

  //TODO this can be deleted
//  private def loopRoadParts(roadNumber: Int): Unit = {
//    var partNumberOpt = RoadAddressDAO.fetchNextRoadPartNumber(roadNumber, 0)
//    while (partNumberOpt.nonEmpty) {
//      val partNumber = partNumberOpt.get
//      val roads = RoadAddressDAO.fetchByRoadPart(roadNumber, partNumber, includeFloating = true)
//      try {
//        val adjusted = LinkRoadAddressCalculator.recalculate(roads)
//        assert(adjusted.lengthCompare(roads.size) == 0) // Must not lose any
//        val (changed, unchanged) = adjusted.partition(ra =>
//          roads.exists(oldra => ra.id == oldra.id && (oldra.startAddrMValue != ra.startAddrMValue || oldra.endAddrMValue != ra.endAddrMValue))
//        )
//        println(s"Road $roadNumber, part $partNumber: ${changed.size} updated, ${unchanged.size} kept unchanged")
//        changed.foreach(addr => RoadAddressDAO.update(addr, None))
//      } catch {
//        case ex: InvalidAddressDataException => println(s"!!! Road $roadNumber, part $partNumber contains invalid address data - part skipped !!!")
//          ex.printStackTrace()
//      }
//      partNumberOpt = RoadAddressDAO.fetchNextRoadPartNumber(roadNumber, partNumber)
//    }
//  }
//
//  def recalculate():Unit = {
//    OracleDatabase.withDynTransaction {
//      var roadNumberOpt = RoadAddressDAO.fetchNextRoadNumber(0)
//      while (roadNumberOpt.nonEmpty) {
//        loopRoadParts(roadNumberOpt.get)
//        roadNumberOpt = RoadAddressDAO.fetchNextRoadNumber(roadNumberOpt.get)
//      }
//    }
//  }

  private def toIntNumber(value: Any): Int = {
    try {
      value match {
        case b: Int => b.intValue()
        case _ => value.asInstanceOf[String].toInt
      }
    } catch {
      case e: Exception => numberThreads
    }
  }

  def importRoadAddresses(importTableName: Option[String]): Unit = {
    println(s"\nCommencing road address import from conversion at time: ${DateTime.now()}")
    val vvhClient = new VVHClient(dr2properties.getProperty("digiroad2.VVHRestApiEndPoint"))
    val geometryAdjustedTimeStamp = dr2properties.getProperty("digiroad2.viite.importTimeStamp", "")
    if (geometryAdjustedTimeStamp == "" || geometryAdjustedTimeStamp.toLong == 0L) {
      println(s"****** Missing or bad value for digiroad2.viite.importTimeStamp in properties: '$geometryAdjustedTimeStamp' ******")
    } else {
      println(s"****** Road address geometry timestamp is $geometryAdjustedTimeStamp ******")
      importTableName match {
        case None => // shouldn't get here because args size test
          throw new Exception("****** Import failed! conversiontable name required as second input ******")
        case Some(tableName) =>
          val importOptions = ImportOptions(
            onlyComplementaryLinks = false,
            useFrozenLinkService = dr2properties.getProperty("digiroad2.VVHRoadlink.frozen", "false").toBoolean,
            geometryAdjustedTimeStamp.toLong, tableName,
            onlyCurrentRoads = dr2properties.getProperty("digiroad2.importOnlyCurrent", "false").toBoolean)
          dataImporter.importRoadAddressData(Conversion.database(), vvhClient, importOptions)

      }
      println(s"Road address import complete at time: ${DateTime.now()}")
    }

  }

  def updateMissingRoadAddresses(): Unit = {
    println(s"\nUpdating missing road address table at time: ${DateTime.now()}")
    val vvhClient = new VVHClient(dr2properties.getProperty("digiroad2.VVHRestApiEndPoint"))
    dataImporter.updateMissingRoadAddresses(vvhClient)
    println(s"Missing address update complete at time: ${DateTime.now()}")
    println()
  }

  def updateRoadAddressesGeometry(): Unit = {
    println(s"\nUpdating road address table geometries at time: ${DateTime.now()}")
    val vVHClient = new VVHClient(dr2properties.getProperty("digiroad2.VVHRestApiEndPoint"))
    dataImporter.updateRoadAddressesGeometry(vvhClient)
    println(s"Road addresses geometry update complete at time: ${DateTime.now()}")
    println()
  }

  def findFloatingRoadAddresses(): Unit = {
    println(s"\nFinding road addresses that are floating at time: ${DateTime.now()}")
    val vvhClient = new VVHClient(dr2properties.getProperty("digiroad2.VVHRestApiEndPoint"))
    val username = properties.getProperty("bonecp.username")
    val roadLinkService = new RoadLinkService(vvhClient, new DummyEventBus, new DummySerializer)
    val roadAddressService = new RoadAddressService(roadLinkService, new DummyEventBus)
    OracleDatabase.withDynTransaction {
      val checker = new FloatingChecker(roadLinkService)
      val roads = checker.checkRoadNetwork(username)
      println(s"${roads.size} segment(s) found")
      roadAddressService.checkRoadAddressFloatingWithoutTX(roads.map(_.id).toSet, float = true)
    }
    println(s"\nRoad Addresses floating field update complete at time: ${DateTime.now()}")
    println()
  }

  private def importComplementaryRoadAddress(): Unit ={
    println(s"\nCommencing complementary road address import at time: ${DateTime.now()}")
    OracleDatabase.withDynTransaction {
      OracleDatabase.setSessionLanguage()
    }
    SqlScriptRunner.runViiteScripts(List(
      "insert_complementary_geometry_data.sql"
    ))
    println(s"complementary road address import completed at time: ${DateTime.now()}")
    println()
  }
  //TODO this can be deleted
  //  private def combineMultipleSegmentsOnLinks(): Unit = {
//    println(s"\nCombining multiple segments on links at time: ${DateTime.now()}")
//    OracleDatabase.withDynTransaction {
//      OracleDatabase.setSessionLanguage()
//      RoadAddressDAO.getAllValidRoadNumbers().foreach(road => {
//        val roadAddresses = RoadAddressDAO.fetchMultiSegmentLinkIds(road).groupBy(_.linkId)
//        val replacements = roadAddresses.mapValues(RoadAddressLinkBuilder.fuseRoadAddress)
//        roadAddresses.foreach { case (linkId, list) =>
//          val currReplacement = replacements(linkId)
//          if (list.lengthCompare(currReplacement.size) != 0) {
//            val (kept, removed) = list.partition(ra => currReplacement.exists(_.id == ra.id))
//            val created = currReplacement.filterNot(ra => kept.exists(_.id == ra.id))
//            RoadAddressDAO.remove(removed)
//            if (created.nonEmpty)
//              RoadAddressDAO.create(created, created.head.createdBy, Some("Automatic_merged"))
//          }
//        }
//      })
//    }
//    println(s"\nFinished the combination of multiple segments on links at time: ${DateTime.now()}")
//  }

  private def importRoadNames() {
    SqlScriptRunner.runViiteScripts(List(
      "roadnames.sql"
    ))
  }

  private def importRoadAddressChangeTestData(): Unit ={
    println(s"\nCommencing road address change test data import at time: ${DateTime.now()}")
    OracleDatabase.withDynTransaction {
      OracleDatabase.setSessionLanguage()
    }
    SqlScriptRunner.runViiteScripts(List(
      "insert_road_address_change_test_data.sql"
    ))
    println(s"Road Address Change Test Data import completed at time: ${DateTime.now()}")
    println()
  }

  private def applyChangeInformationToRoadAddressLinks(numThreads: Int): Unit = {
    throw new NotImplementedError("Will be implemented at VIITE-1536")

    //    val roadLinkService = new RoadLinkService(vvhClient, new DummyEventBus, new JsonSerializer)
//    val roadAddressService = new RoadAddressService(roadLinkService, new DummyEventBus)
//
//    println("Clearing cache...")
//    roadLinkService.clearCache()
//    println("Cache cleaned.")
//
//    //Get All Municipalities
//    val municipalities: ParSet[Long] =
//      OracleDatabase.withDynTransaction {
//        MunicipalityDAO.getMunicipalityMapping.keySet
//      }.par
//
//    //For each municipality get all VVH Roadlinks
//    municipalities.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(numThreads))
//    municipalities.map { municipality =>
//      println("Start processing municipality %d".format(municipality))
//
//      //Obtain all RoadLink by municipality and change info from VVH
//      val (roadLinks, changedRoadLinks) = roadLinkService.getFrozenRoadLinksAndChangesFromVVH(municipality.toInt,properties.getProperty("digiroad2.VVHRoadlink.frozen", "false").toBoolean)
//      println ("Total roadlink for municipality " + municipality + " -> " + roadLinks.size)
//      println ("Total of changes for municipality " + municipality + " -> " + changedRoadLinks.size)
//      if(roadLinks.nonEmpty) {
//        val changedLinkIds = changedRoadLinks.map(c => c.oldId.getOrElse(c.newId.getOrElse(0L))).toSet
//        //  Get road address from viite DB from the roadLinks ids
//        val roadAddresses: List[RoadAddress] =  OracleDatabase.withDynTransaction {
//          RoadAddressDAO.fetchByLinkId(changedLinkIds, includeTerminated = false)
//        }
//        try {
//          val groupedAddresses = roadAddresses.groupBy(_.linkId)
//          val timestamps = groupedAddresses.mapValues(_.map(_.adjustedTimestamp).min)
//          val affectingChanges = changedRoadLinks.filter(ci => timestamps.get(ci.oldId.getOrElse(ci.newId.get)).nonEmpty && ci.vvhTimeStamp >= timestamps.getOrElse(ci.oldId.getOrElse(ci.newId.get), 0L))
//          println ("Affecting changes for municipality " + municipality + " -> " + affectingChanges.size)
//          roadAddressService.applyChanges(roadLinks, affectingChanges, roadAddresses)
//        } catch {
//          case e: Exception => println("ERR! -> " + e.getMessage)
//        }
//      }
//      println("End processing municipality %d".format(municipality))
//    }

  }

  private def updateProjectLinkGeom(): Unit = {
    val roadLinkService = new RoadLinkService(vvhClient, new DummyEventBus, new DummySerializer)
    val roadAddressService = new RoadAddressService(roadLinkService, new DummyEventBus)
    val projectService = new  ProjectService(roadAddressService,roadLinkService, new DummyEventBus)
    val projectsIDs= projectService.getRoadAddressAllProjects.map(x=>x.id)
    val projectCount=projectsIDs.size
    var c=0
    projectsIDs.foreach(x=>
    {
      c+=1
      println("Updating Geometry for project " +c+ "/"+projectCount)
      projectService.updateProjectLinkGeometry(x,"BJ")
    })

  }

  private def correctNullElyCodeProjects(): Unit = {
    val roadLinkService = new RoadLinkService(vvhClient, new DummyEventBus, new DummySerializer)
    val roadAddressService = new RoadAddressService(roadLinkService, new DummyEventBus)
    val projectService = new  ProjectService(roadAddressService,roadLinkService, new DummyEventBus)
    val startTime = DateTime.now()
    println(s"Starting project Ely code correct now")
    projectService.correctNullProjectEly()
    println(s"Project Ely's correct in  ${hms.print(new Period(startTime, DateTime.now()))}")
  }


  private def updateRoadAddressGeometrySource(): Unit = {
    throw new NotImplementedError("Will be implemented at VIITE-1554")

    //    val roadLinkService = new RoadLinkService(vvhClient, new DummyEventBus, new DummySerializer)
//
//    //Get All Roads
//    val roads: Seq[Long] =
//      OracleDatabase.withDynTransaction {
//        RoadAddressDAO.getAllValidRoadNumbers()
//      }
//
//    //For each municipality get all VVH Roadlinks
//    roads.par.foreach { road =>
//      println("%d: Fetch road addresses for road #%d".format(road, road))
//      OracleDatabase.withDynTransaction {
//        val roadAddressSeq = RoadAddressDAO.fetchByRoad(road)
//        // Floating addresses are ignored
//        val linkIds = roadAddressSeq.map(_.linkId).toSet
//        println("%d: %d address rows fetched on %d links".format(road, roadAddressSeq.size, linkIds.size))
//        val cacLinks = roadLinkService.getCurrentAndComplementaryVVHRoadLinks(linkIds)
//          .map(rl => rl.linkId -> rl.linkSource).toMap
//        // If not present in current and complementary, check the historic links, too
//        val vvhHistoryLinks = roadLinkService.getRoadLinksHistoryFromVVH(linkIds -- cacLinks.keySet)
//          .map(rl => rl.linkId -> LinkGeomSource.HistoryLinkInterface).toMap
//        val vvhLinks = cacLinks ++ vvhHistoryLinks
//        val updated = roadAddressSeq
//          .filterNot(ra => vvhLinks.getOrElse(ra.linkId, ra.linkGeomSource) == ra.linkGeomSource)
//          .count(ra =>
//            RoadAddressDAO.updateLinkSource(ra.id, vvhLinks(ra.linkId))
//          )
//        println("%d: %d addresses updated".format(road, updated))
//      }
//    }

  }

  //TODO check if this will continue to be needed
//  def checkLinearLocation(): Unit = {
//    OracleDatabase.withDynTransaction {
//      val roadwayIds = RoadAddressDAO.getRoadwayIdsFromRoadAddress
//      println(s"Found a total of ${roadwayIds.size} roadway ids")
//      val chunks = generateCommonIdChunks(roadwayIds, 1000)
//      chunks.par.foreach {
//        case (min, max) =>
//          println(s"Processing roadway ids from $min to $max")
//          val roads = RoadAddressDAO.getRoadAddressByFilter(RoadAddressDAO.withRoadwayIds(min, max))
//          roads.groupBy(_.roadwayId).foreach { group =>
//            val dateTimeLines = group._2.map(_.startDate).distinct
//
//            val mappedTimeLines: Seq[TimeLine] = dateTimeLines.flatMap {
//              date =>
//                val groupedAddresses: Seq[TimeLine] = group._2.groupBy(g => (g.roadNumber, g.roadPartNumber)).map { roadAddresses =>
//                  val filteredAdresses: Seq[RoadAddress] = roadAddresses._2.filter { ra => ra.validTo.isEmpty && (date.get.getMillis >= ra.startDate.get.getMillis) && (ra.endDate.isEmpty || date.get.getMillis < ra.endDate.get.getMillis) }
//                  val addrLength = filteredAdresses.map(_.endAddrMValue).sum - filteredAdresses.map(_.startAddrMValue).sum
//                  TimeLine(addrLength, filteredAdresses)
//                }.filter(_.addresses.nonEmpty).toSeq
//                groupedAddresses
//            }
//
//            val roadErrors = if (mappedTimeLines.size > 1) {
//              val errors: Set[RoadAddress] = mappedTimeLines.sliding(2).flatMap { case Seq(first, second) => {
//                if (first.addressLength != second.addressLength) {
//                  first.addresses.toSet
//                }
//                else {
//                  Set.empty[RoadAddress]
//                }
//              }
//              }.toSet
//              errors
//            } else {
//              Set.empty[RoadAddress]
//            }
//            println(s"Found ${roadErrors.size} errors for common_history_id ${group._2.head.roadwayId}")
//            val lastVersion = getLatestRoadNetworkVersionId
//
//            roadErrors.filter { road =>
//              val error = RoadNetworkDAO.getRoadNetworkError(road.id, InconsistentLrmHistory)
//              error.isEmpty || error.get.network_version != lastVersion
//            }.foreach(error => RoadNetworkDAO.addRoadNetworkError(error.id, InconsistentLrmHistory.value))
//          }
//      }
//    }
//  }

  //TODO check if this will be needed
//  def fuseRoadAddressWithHistory(): Unit = {
//
//    val roadLinkService = new RoadLinkService(vvhClient, new DummyEventBus, new DummySerializer)
//    val roadAddressService = new RoadAddressService(roadLinkService, new DummyEventBus)
//    val elyCodes = OracleDatabase.withDynSession {
//      MunicipalityDAO.getMunicipalityMapping.values.toSet
//    }
//
//    elyCodes.foreach(ely => {
//      println(s"Going to fuse roads for ely $ely")
//      val roads = OracleDatabase.withDynSession {
//        RoadAddressDAO.getRoadAddressByEly(ely)
//      }
//      println(s"Got ${roads.size} addresses for ely $ely")
//      val fusedRoadAddresses = RoadAddressLinkBuilder.fuseRoadAddressWithTransaction(roads)
//      val kept = fusedRoadAddresses.map(_.id).toSet
//      val removed = roads.map(_.id).toSet.diff(kept)
//      val roadAddressesToRegister = fusedRoadAddresses.filter(_.id == fi.liikennevirasto.viite.NewRoadAddress)
//      println(s"Fusing ${roadAddressesToRegister.size} roads for ely $ely")
//      if (roadAddressesToRegister.nonEmpty)
//        roadAddressService.mergeRoadAddressHistory(RoadAddressMerge(removed, roadAddressesToRegister))
//    })
//  }

  private def showFreezeInfo(): Unit = {
    println("Road link geometry freeze is active; exiting without changes")
  }

  def flyway: Flyway = {
    val flyway = new Flyway()
    flyway.setDataSource(ds)
    flyway.setInitVersion("-1")
    flyway.setInitOnMigrate(true)
    flyway.setLocations("db.migration")
    flyway
  }

  def migrateAll(): Int = {
    flyway.migrate()
  }

  def tearDown() {
    flyway.clean()
  }

  def setUpTest() {
    migrateAll()
    SqlScriptRunner.runScripts(List(
      "insert_users.sql",
      "test_fixture_sequences.sql",
      "insert_road_address_data.sql",
      "insert_floating_road_addresses.sql",
      "insert_overlapping_road_addresses.sql", // Test data for OverLapDataFixture (VIITE-1518)
      "insert_project_link_data.sql",
      "insert_road_names.sql"
    ))
  }

  def importMunicipalityCodes() {
    println("\nCommencing municipality code import at time: ")
    println(DateTime.now())
    new MunicipalityCodeImporter().importMunicipalityCodes()
    println("Municipality code import complete at time: ")
    println(DateTime.now())
    println("\n")
  }

  def main(args: Array[String]): Unit = {
    import scala.util.control.Breaks._
    val username = properties.getProperty("bonecp.username")
    if (!username.startsWith("dr2dev")) {
      println("*************************************************************************************")
      println("YOU ARE RUNNING FIXTURE RESET AGAINST A NON-DEVELOPER DATABASE, TYPE 'YES' TO PROCEED")
      println("*************************************************************************************")
      breakable {
        while (true) {
          val input = Console.readLine()
          if (input.trim() == "YES") {
            break()
          }
        }
      }
    }

    args.headOption match {
      case Some("find_floating_road_addresses") if geometryFrozen =>
        showFreezeInfo()
      case Some("find_floating_road_addresses") =>
        findFloatingRoadAddresses()
      case Some("import_road_addresses") =>
        if (args.length > 1)
          importRoadAddresses(Some(args(1)))
        else
          throw new Exception("****** Import failed! conversiontable name required as second input ******")
      case Some("import_complementary_road_address") =>
        importComplementaryRoadAddress()
      case Some("update_missing") if geometryFrozen =>
        showFreezeInfo()
      case Some("update_missing") =>
        updateMissingRoadAddresses()
//      case Some("fuse_multi_segment_road_addresses") =>
//        combineMultipleSegmentsOnLinks()
      case Some("update_road_addresses_geometry") =>
        updateRoadAddressesGeometry()
      case Some("import_road_address_change_test_data") =>
        importRoadAddressChangeTestData()
      case Some("apply_change_information_to_road_address_links") if geometryFrozen =>
        showFreezeInfo()
      case Some("apply_change_information_to_road_address_links") =>
        val numThreads = if (args.length > 1) toIntNumber(args(1)) else numberThreads
        applyChangeInformationToRoadAddressLinks(numThreads)
      case Some("update_road_address_link_source") if geometryFrozen =>
        showFreezeInfo()
      case Some("update_road_address_link_source") =>
        updateRoadAddressGeometrySource()
      case Some("update_project_link_geom") =>
        updateProjectLinkGeom()
      case Some("import_road_names") =>
        importRoadNames()
      case Some("correct_null_ely_code_projects") =>
        correctNullElyCodeProjects()
//      case Some("check_lrm_position") =>
//        checkLinearLocation()
//      case Some("fuse_road_address_with_history") =>
//        fuseRoadAddressWithHistory()
      case Some("revert_overlapped_road_addresses") =>
        val options = args.tail
        val save = options.contains("save")
        val fixAddressValues = options.contains("fix-address-values")
        val withPartialOverlap = options.contains("with-partial-overlap")
        val fetchAllChangesFromVVH = options.contains("fetch-all-changes-from-vvh")
        val addressThreshold = options.find(_.startsWith("address-threshold=")).map(_.replace("address-threshold=", "").toInt).getOrElse(6)
        OracleDatabase.withDynTransaction {
          val overlapDataFixture = new OverlapDataFixture(vvhClient)
          overlapDataFixture.fixOverlapRoadAddresses(dryRun = !save, fixAddressValues, withPartialOverlap, fetchAllChangesFromVVH, addressThreshold)
        }
      case Some("revert_overlapped_road_addresses_by_date") =>
        val options = args.tail
        val save = options.contains("save")
        val addressSectionThreshold = options.find(_.startsWith("address-section-threshold=")).map(_.replace("address-section-threshold=", "").toInt).getOrElse(10)
          val overlapDataFixture = new OverlapDataFixture(vvhClient)
          overlapDataFixture.fixOverlapRoadAddressesByDates(dryRun = !save, addressSectionThreshold)
      case Some("test") =>
        tearDown()
        setUpTest()
        importMunicipalityCodes()

      case _ => println("Usage: DataFixture import_road_addresses <conversion table name> | recalculate_addresses | update_missing " +
        "| find_floating_road_addresses | import_complementary_road_address | fuse_multi_segment_road_addresses " +
        "| update_road_addresses_geometry_no_complementary | update_road_addresses_geometry | import_road_address_change_test_data " +
        "| apply_change_information_to_road_address_links | update_road_address_link_source | correct_null_ely_code_projects | import_road_names " +
        "| fuse_road_address_with_history | check_lrm_position | revert_overlapped_road_addresses")
    }
  }

  case class TimeLine(addressLength: Long, addresses: Seq[RoadAddress])
  private def generateCommonIdChunks(ids: Seq[Long], chunkNumber: Long): Seq[(Long, Long)] = {
    val (chunks, _) = ids.foldLeft((Seq[Long](0), 0)) {
      case ((fchunks, index), linkId) =>
        if (index > 0 && index % chunkNumber == 0) {
          (fchunks ++ Seq(linkId), index + 1)
        } else {
          (fchunks, index + 1)
        }
    }
    val result = if (chunks.last == ids.last) {
      chunks
    } else {
      chunks ++ Seq(ids.last)
    }

    result.zip(result.tail)
  }
}
