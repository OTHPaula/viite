package fi.liikennevirasto.digiroad2

import java.lang.management.ManagementFactory
import java.util.Properties

import fi.liikennevirasto.viite.process.ProjectSectionCalculator.getClass
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import org.eclipse.jetty.client.api.Request
import org.eclipse.jetty.client.{HttpClient, HttpProxy}
import org.eclipse.jetty.jmx.MBeanContainer
import org.eclipse.jetty.proxy.ProxyServlet
import org.eclipse.jetty.server.handler.ContextHandlerCollection
import org.eclipse.jetty.server.{Handler, Server}
import org.eclipse.jetty.webapp.WebAppContext
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

trait DigiroadServer {
  val viiteContextPath: String

  def startServer() {
    val server = new Server(9080)
    val handler = new ContextHandlerCollection()
    val mbContainer = new MBeanContainer(ManagementFactory.getPlatformMBeanServer)
    server.addEventListener(mbContainer)
    server.addBean(mbContainer)
    val handlers = Array(createViiteContext())
    handler.setHandlers(handlers.map(_.asInstanceOf[Handler]))
    server.setHandler(handler)
    server.start()
    server.join()
  }

  def createViiteContext() = {
    val appContext = new WebAppContext()
    val properties = new Properties()
    properties.load(getClass.getResourceAsStream("/digiroad2.properties"))
    appContext.setDescriptor("src/main/webapp/WEB-INF/web.xml")
    appContext.setResourceBase("src/main/webapp/viite")
    appContext.setContextPath(viiteContextPath)
    appContext.setParentLoaderPriority(true)
    appContext.setInitParameter("org.eclipse.jetty.servlet.Default.dirAllowed", "false")
    appContext.addServlet(classOf[NLSProxyServlet], "/maasto/*")
    appContext.addServlet(classOf[MapTileProxyServlet], "/mapTest/*")
    appContext.addServlet(classOf[ArcGisProxyServlet], "/arcgis/*")
    appContext.addServlet(classOf[VKMProxyServlet], "/vkm/*")
    appContext.addServlet(classOf[VKMUIProxyServlet], "/viitekehysmuunnin/*")
    appContext.getMimeTypes.addMimeMapping("ttf", "application/x-font-ttf")
    appContext.getMimeTypes.addMimeMapping("woff", "application/x-font-woff")
    appContext.getMimeTypes.addMimeMapping("eot", "application/vnd.ms-fontobject")
    appContext.getMimeTypes.addMimeMapping("js", "application/javascript; charset=UTF-8")
    appContext
  }
}

class NLSProxyServlet extends ProxyServlet {

  def regex = "/(digiroad|viite)".r

  override def rewriteURI(req: HttpServletRequest): java.net.URI = {
    val uri = req.getRequestURI
    java.net.URI.create("http://karttamoottori.maanmittauslaitos.fi"
      + regex.replaceFirstIn(uri, ""))
  }

  override def sendProxyRequest(clientRequest: HttpServletRequest, proxyResponse: HttpServletResponse, proxyRequest: Request): Unit = {
    proxyRequest.header("Referer", "http://www.paikkatietoikkuna.fi/web/fi/kartta")
    proxyRequest.header("Host", null)
    super.sendProxyRequest(clientRequest, proxyResponse, proxyRequest)
  }
//not sure if following is still needed
  override def getHttpClient: HttpClient = {
    val client = super.getHttpClient
    val properties = new Properties()
    properties.load(getClass.getResourceAsStream("/digiroad2.properties"))
    if (properties.getProperty("http.proxySet", "false").toBoolean) {
      val proxy = new HttpProxy(properties.getProperty("http.proxyHost", "localhost"), properties.getProperty("http.proxyPort", "80").toInt)
      proxy.getExcludedAddresses.addAll(properties.getProperty("http.nonProxyHosts", "").split("|").toList)
      client.getProxyConfiguration.getProxies.add(proxy)
      client.setIdleTimeout(60000)
    }
    client
  }
}

class MapTileProxyServlet extends ProxyServlet {
  def regex = "/(digiroad|viite)/mapTest/".r
  private val logger = LoggerFactory.getLogger(getClass)

  override def rewriteURI(req: HttpServletRequest): java.net.URI = {
    val url = "http://oag.liikennevirasto.fi/" + regex.replaceFirstIn(req.getRequestURI, "")
    logger.info("MapTileProxyServlet->rewriteURI "+url)
    java.net.URI.create(url)
  }

  override def sendProxyRequest(clientRequest: HttpServletRequest, proxyResponse: HttpServletResponse, proxyRequest: Request): Unit = {
    super.sendProxyRequest(clientRequest, proxyResponse, proxyRequest)
  }
}

class ArcGisProxyServlet extends ProxyServlet {
  val logger = LoggerFactory.getLogger(getClass)
  override def rewriteURI(req: HttpServletRequest): java.net.URI = {
    val uri = req.getRequestURI
    java.net.URI.create("http://aineistot.esri.fi"
      + uri.replaceFirst("/viite", ""))
  }

  override def sendProxyRequest(clientRequest: HttpServletRequest, proxyResponse: HttpServletResponse, proxyRequest: Request): Unit = {
    proxyRequest.header("Referer", null)
    proxyRequest.header("Host", null)
    proxyRequest.header("Cookie", null)
    proxyRequest.header("OAM_REMOTE_USER", null)
    proxyRequest.header("OAM_IDENTITY_DOMAIN", null)
    proxyRequest.header("OAM_LAST_REAUTHENTICATION_TIME", null)
    proxyRequest.header("OAM_GROUPS", null)
    proxyRequest.header("X-Forwarded-Host", null)
    proxyRequest.header("X-Forwarded-Server", null)
    proxyRequest.header("Via", null)
    super.sendProxyRequest(clientRequest, proxyResponse, proxyRequest)
  }

  override def getHttpClient: HttpClient = {
    val client = super.getHttpClient
    val properties = new Properties()
    properties.load(getClass.getResourceAsStream("/digiroad2.properties"))
    if (properties.getProperty("http.proxySet", "false").toBoolean) {
      val proxy = new HttpProxy("127.0.0.1", 3128)
      proxy.getExcludedAddresses.addAll(properties.getProperty("http.nonProxyHosts", "").split("|").toList)
      client.getProxyConfiguration.getProxies.add(proxy)
      client.setIdleTimeout(60000)
    }
    client
  }
}

class VKMProxyServlet extends ProxyServlet {
  def regex = "/(digiroad|viite)".r

  override def rewriteURI(req: HttpServletRequest): java.net.URI = {
    val properties = new Properties()
    properties.load(getClass.getResourceAsStream("/digiroad2.properties"))
    val vkmUrl: String = properties.getProperty("digiroad2.VKMUrl")
    java.net.URI.create(vkmUrl + regex.replaceFirstIn(req.getRequestURI, ""))
  }

  override def sendProxyRequest(clientRequest: HttpServletRequest, proxyResponse: HttpServletResponse, proxyRequest: Request): Unit = {
    val parameters = clientRequest.getParameterMap
    parameters.foreach { case(key, value) =>
      proxyRequest.param(key, value.mkString(""))
    }
    super.sendProxyRequest(clientRequest, proxyResponse, proxyRequest)
  }
}

class VKMUIProxyServlet extends ProxyServlet {
  def regex = "/(digiroad|viite)/viitekehysmuunnin/".r

  override def rewriteURI(req: HttpServletRequest): java.net.URI = {
    java.net.URI.create("http://localhost:3000" + regex.replaceFirstIn(req.getRequestURI, ""))
  }
}
