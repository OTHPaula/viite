package fi.liikennevirasto.digiroad2.linearasset

import com.vividsolutions.jts.geom.LineSegment
import fi.liikennevirasto.digiroad2.GeometryUtils
import org.geotools.graph.build.line.BasicLineGraphGenerator
import org.geotools.graph.structure.Graph
import scala.collection.JavaConversions._

trait GraphPartitioner {
  protected def clusterLinks[T <: PolyLine](links: Seq[T]): Seq[Graph] = {
    val generator = new BasicLineGraphGenerator(0.5)
    links.foreach { link =>
      val (sp, ep) = GeometryUtils.geometryEndpoints(link.geometry)
      val segment = new LineSegment(sp.x, sp.y, ep.x, ep.y)
      val graphable = generator.add(segment)
      graphable.setObject(link)
    }
    clusterGraph(generator.getGraph)
  }

  private def clusterGraph(graph: Graph): Seq[Graph] = {
    val partitioner = new org.geotools.graph.util.graph.GraphPartitioner(graph)
    partitioner.partition()
    partitioner.getPartitions.toList.asInstanceOf[List[Graph]]
  }
}
