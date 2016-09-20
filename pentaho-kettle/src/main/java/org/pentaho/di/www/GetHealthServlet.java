/*! ******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.www;

import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.job.Job;
import org.pentaho.di.trans.Trans;
import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.GlobalMemory;
import oshi.hardware.HardwareAbstractionLayer;
import oshi.software.os.OperatingSystem;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

public class GetHealthServlet extends BaseHttpServlet implements CartePluginInterface {
    private static Class<?> PKG = GetHealthServlet.class; // for i18n purposes, needed by Translator2!!

    private static final long serialVersionUID = -4155334859966384208L;

    public static final String CONTEXT_PATH = "/kettle/health";

    static String buildServerHealthXml(String serverName, boolean isMaster, JobMap jobMap, TransformationMap transMap) {
        Runtime jvmRuntime = Runtime.getRuntime();
        int jvmCores = jvmRuntime.availableProcessors();
        long jvmTotalMemory = jvmRuntime.totalMemory();
        long jvmFreeMemory = jvmRuntime.freeMemory();

        SystemInfo sysInfo = new SystemInfo();
        HardwareAbstractionLayer hardware = sysInfo.getHardware();
        CentralProcessor processor = hardware.getProcessor();
        long sysUptime = processor.getSystemUptime();
        double sysCpuLoad = processor.getSystemCpuLoad();
        double sysLoadAvg = processor.getSystemLoadAverage();
        int sysPhysicalCores = processor.getPhysicalProcessorCount();
        int sysLogicalCores = processor.getLogicalProcessorCount();

        GlobalMemory memory = hardware.getMemory();
        long sysTotalMemory = memory.getTotal();
        long sysFreeMemory = memory.getAvailable();
        long sysSwapTotal = memory.getSwapTotal();
        long sysSwapUsed = memory.getSwapUsed();

        File root = new File("/");
        long totalDiskSpace = root.getTotalSpace();
        long usableDiskSpace = root.getUsableSpace();

        OperatingSystem os = sysInfo.getOperatingSystem();
        int sysProcessCount = os.getProcessCount();
        int sysThreadCount = os.getThreadCount();

        return buildServerStatusXml(serverName, isMaster, sysUptime, jvmCores, sysPhysicalCores, sysLogicalCores,
                sysCpuLoad, sysLoadAvg, jvmTotalMemory, jvmFreeMemory, sysTotalMemory, sysFreeMemory,
                sysSwapTotal, sysSwapUsed, totalDiskSpace, usableDiskSpace, sysProcessCount, sysThreadCount,
                jobMap, transMap);
    }

    static String buildDummyServerStatusXml(String serverName) {
        return buildServerStatusXml(serverName, false, 0L, 0, 0, 0, 0.0D, 0.0D, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0, 0,
                null, null);
    }

    static String buildServerStatusXml(String serverName, boolean serverType,
                                       long sysUptime, int jvmCores,
                                       int sysPhysicalCores, int sysLogicalCores,
                                       double sysCpuLoad, double sysLoadAvg,
                                       long jvmTotalMemory, long jvmFreeMemory,
                                       long sysTotalMemory, long sysFreeMemory,
                                       long sysSwapTotal, long sysSwapUsed,
                                       long totalDiskSpace, long usableDiskSpace,
                                       int sysProcessCount, int sysThreadCount,
                                       JobMap jobMap, TransformationMap transMap) {
        StringBuilder xml = new StringBuilder();

        xml.append("<server_status>")
                .append(XMLHandler.addTagValue("server_name", serverName == null ? "<N/A>" : serverName))
                .append(XMLHandler.addTagValue("server_type", serverType ? "master" : "slave"))
                .append(XMLHandler.addTagValue("sys_up_time", sysUptime < 0L ? 0L : sysUptime))
                .append(XMLHandler.addTagValue("jvm_cores", jvmCores < 0 ? 0 : jvmCores))
                .append(XMLHandler.addTagValue("sys_physical_cores", sysPhysicalCores < 0 ? 0 : sysPhysicalCores))
                .append(XMLHandler.addTagValue("sys_logical_cores", sysLogicalCores < 0 ? 0 : sysLogicalCores))
                .append(XMLHandler.addTagValue("sys_cpu_load", sysCpuLoad < 0.0D ? 0.0D : sysCpuLoad))
                .append(XMLHandler.addTagValue("sys_load_avg", sysLoadAvg < 0.0D ? 0.0D : sysLoadAvg))
                .append(XMLHandler.addTagValue("jvm_mem_total", jvmTotalMemory < 0L ? 0L : jvmTotalMemory))
                .append(XMLHandler.addTagValue("jvm_mem_free", jvmFreeMemory < 0L ? 0L : jvmFreeMemory))
                .append(XMLHandler.addTagValue("sys_mem_total", sysTotalMemory < 0L ? 0L : sysTotalMemory))
                .append(XMLHandler.addTagValue("sys_mem_free", sysFreeMemory < 0L ? 0L : sysFreeMemory))
                .append(XMLHandler.addTagValue("sys_swap_total", sysSwapTotal < 0L ? 0L : sysSwapTotal))
                .append(XMLHandler.addTagValue("sys_swap_used", sysSwapUsed < 0L ? 0L : sysSwapUsed))
                .append(XMLHandler.addTagValue("sys_disk_total", totalDiskSpace < 0L ? 0L : totalDiskSpace))
                .append(XMLHandler.addTagValue("sys_disk_free", usableDiskSpace < 0L ? 0L : usableDiskSpace))
                .append(XMLHandler.addTagValue("sys_process_count", sysProcessCount < 0 ? 0 : sysProcessCount))
                .append(XMLHandler.addTagValue("sys_thread_count", sysThreadCount < 0 ? 0 : sysThreadCount))
                .append("</server_status>");

        return xml.toString();
    }

    static String getServerName(SlaveServer server) {
        String serverName = null;

        if (server != null) {
            serverName = server.getServerAndPort();
        }

        return serverName;
    }

    public GetHealthServlet() {
    }

    public GetHealthServlet(TransformationMap transformationMap, JobMap jobMap) {
        super(transformationMap, jobMap);
    }

    public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException,
            IOException {
        if (isJettyMode() && !request.getContextPath().startsWith(CONTEXT_PATH)) {
            return;
        }

        response.setStatus(HttpServletResponse.SC_OK);

        response.setContentType("text/xml");
        response.setCharacterEncoding(Const.XML_ENCODING);

        SlaveServer currentServer = CarteSingleton.getSlaveServerConfig().getSlaveServer();
        String currentServerName = getServerName(currentServer);

        PrintWriter out = response.getWriter();

        boolean partialXml = "Y".equalsIgnoreCase(request.getParameter("partial"));

        if (!partialXml) {
            out.print(XMLHandler.getXMLHeader(Const.XML_ENCODING));

            StringBuilder xml = new StringBuilder();
            xml.append("<server_status_list>").append(buildServerHealthXml(currentServerName,
                    currentServer.getHostname() == null || currentServer.isMaster(),
                    getJobMap(), getTransformationMap()));

            // let's show health of the whole cluster regardless how many servers we have
            // FIXME this might be slow but should be fine for a small cluster with 5 - 10 nodes
            List<SlaveServerDetection> detections = getDetections();
            if (detections != null) {
                for (SlaveServerDetection detectedServer : detections) {
                    SlaveServer server = detectedServer.getSlaveServer();
                    try {
                        xml.append(
                                server.execService(CONTEXT_PATH + "?partial=Y&source=" + currentServer.getName()));
                    } catch (Exception e) {
                        xml.append(buildDummyServerStatusXml(getServerName(server)));
                    }
                }
            }

            xml.append("</server_status_list>");
            out.print(xml.toString());
        } else {
            out.print(buildServerHealthXml(currentServerName,
                    currentServer.getHostname() == null || currentServer.isMaster(),
                    getJobMap(), getTransformationMap()));
        }
    }

    public String toString() {
        return "Health Handler";
    }

    public String getService() {
        return CONTEXT_PATH + " (" + toString() + ")";
    }

    public String getContextPath() {
        return CONTEXT_PATH;
    }

}
