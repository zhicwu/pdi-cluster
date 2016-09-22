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
import oshi.hardware.NetworkIF;
import oshi.software.os.OSProcess;
import oshi.software.os.OperatingSystem;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.pentaho.di.www.GetStatusServlet.JOB_NAME_PARAMS;

public class GetHealthServlet extends BaseHttpServlet implements CartePluginInterface {
    private static final Class<?> PKG = GetHealthServlet.class; // for i18n purposes, needed by Translator2!!

    private static final long serialVersionUID = -4155334859966384208L;

    private static final String ROOT_DIRECTORY = "/";

    private static final String XML_CONTENT_TYPE = "text/xml";
    private static final String PARAM_PARTIAL = "partial";
    private static final String PARAM_PARTIAL_YES = "Y";

    private static final String DEFAULT_SERVER_NAME = "n/a";
    private static final String MASTER_SERVER = "master";
    private static final String SLAVE_SERVER = "slave";

    private static final String TAG_SERVER_STATUS_LIST_BEGIN = "<server_status_list>";
    private static final String TAG_SERVER_STATUS_LIST_END = "</server_status_list>";
    private static final String TAG_SERVER_STATUS_BEGIN = "<server_status>";
    private static final String TAG_SERVER_STATUS_END = "</server_status>";
    private static final String TAG_SERVER_NAME = "server_name";
    private static final String TAG_SERVER_TYPE = "server_type";
    private static final String TAG_SYS_UP_TIME = "sys_up_time";
    private static final String TAG_PROC_UP_TIME = "proc_up_time";
    private static final String TAG_JVM_CORES = "jvm_cores";
    private static final String TAG_SYS_PHYSICAL_CORES = "sys_physical_cores";
    private static final String TAG_SYS_LOGICAL_CORES = "sys_logical_cores";
    private static final String TAG_SYS_CPU_LOAD = "sys_cpu_load";
    private static final String TAG_SYS_LOAD_AVG = "sys_load_avg";
    private static final String TAG_JVM_MEM_TOTAL = "jvm_mem_total";
    private static final String TAG_JVM_MEM_FREE = "jvm_mem_free";
    private static final String TAG_SYS_MEM_TOTAL = "sys_mem_total";
    private static final String TAG_SYS_MEM_FREE = "sys_mem_free";
    private static final String TAG_SYS_SWAP_TOTAL = "sys_swap_total";
    private static final String TAG_SYS_SWAP_USED = "sys_swap_used";
    private static final String TAG_BYTE_RECEIVED = "bytes_received";
    private static final String TAG_BYTE_SENT = "bytes_sent";
    private static final String TAG_SYS_DISK_TOTAL = "sys_disk_total";
    private static final String TAG_SYS_DISK_FREE = "sys_disk_free";
    private static final String TAG_SYS_PROCESSES = "sys_processes";
    private static final String TAG_SYS_THREADS = "sys_threads";
    private static final String TAG_TOTAL_JOBS = "total_jobs";
    private static final String TAG_UNIQUE_JOBS = "unique_jobs";
    private static final String TAG_RUNNING_JOBS = "running_jobs";
    private static final String TAG_STOPPED_JOBS = "stopped_jobs";
    private static final String TAG_FAILED_JOBS = "failed_jobs";
    private static final String TAG_FINISHED_JOBS = "finished_jobs";
    private static final String TAG_HALTED_JOBS = "halted_jobs";
    private static final String TAG_TOTAL_TRANS = "total_trans";
    private static final String TAG_RUNNING_TRANS = "running_trans";
    private static final String TAG_PAUSED_TRANS = "paused_trans";
    private static final String TAG_STOPPED_TRANS = "stopped_trans";
    private static final String TAG_FAILED_TRANS = "failed_trans";
    private static final String TAG_FINISHED_TRANS = "finished_trans";
    private static final String TAG_HALTED_TRANS = "halted_trans";
    private static final String TAG_ELAPSED_TIME = "elapsed_time";

    private static String SERVER_NAME;

    public static final String CONTEXT_PATH = "/kettle/health";
    public static final String SLAVE_CONTEXT_PATH = CONTEXT_PATH + "?" + PARAM_PARTIAL + "=" + PARAM_PARTIAL_YES;

    static NetworkIF getNetworkInterface(HardwareAbstractionLayer hardware) {
        NetworkIF nif = null;

        for (NetworkIF i : hardware.getNetworkIFs()) {
            NetworkInterface n = i.getNetworkInterface();
            try {
                if (n.isUp() && !n.isLoopback() && !n.isPointToPoint() && !n.isVirtual()) {
                    nif = i;
                    break;
                }
            } catch (Exception e) {
                // it's just about selecting the proper network interface
            }
        }

        return nif;
    }

    static String buildServerHealthXml(String serverName, boolean isMaster, JobMap jobMap, TransformationMap transMap) {
        Runtime jvmRuntime = Runtime.getRuntime();
        int jvmCores = jvmRuntime.availableProcessors();
        long jvmTotalMemory = jvmRuntime.totalMemory();
        long jvmFreeMemory = jvmRuntime.freeMemory();

        SystemInfo sysInfo = new SystemInfo();
        HardwareAbstractionLayer hardware = sysInfo.getHardware();
        CentralProcessor processor = hardware.getProcessor();
        long sysUptime = processor.getSystemUptime();
        double sysCpuLoad = new BigDecimal(processor.getSystemCpuLoad())
                .setScale(2, RoundingMode.HALF_UP).doubleValue();

        double sysLoadAvg = processor.getSystemLoadAverage();
        int sysPhysicalCores = processor.getPhysicalProcessorCount();
        int sysLogicalCores = processor.getLogicalProcessorCount();

        GlobalMemory memory = hardware.getMemory();
        long sysTotalMemory = memory.getTotal();
        long sysFreeMemory = memory.getAvailable();
        long sysSwapTotal = memory.getSwapTotal();
        long sysSwapUsed = memory.getSwapUsed();

        NetworkIF nif = getNetworkInterface(hardware);
        long bytesReceived = nif == null ? 0L : nif.getBytesRecv();
        long bytesSent = nif == null ? 0L : nif.getPacketsSent();

        File root = new File(ROOT_DIRECTORY);
        long totalDiskSpace = root.getTotalSpace();
        long usableDiskSpace = root.getUsableSpace();

        OperatingSystem os = sysInfo.getOperatingSystem();
        long processUptime = 0L;
        for (OSProcess process : os.getProcesses(0, OperatingSystem.ProcessSort.PID)) {
            if (process.getProcessID() == os.getProcessId()) {
                processUptime = process.getUpTime();
                break;
            }
        }

        int sysProcessCount = os.getProcessCount();
        int sysThreadCount = os.getThreadCount();

        return buildServerStatusXml(serverName, isMaster, sysUptime, jvmCores, sysPhysicalCores, sysLogicalCores,
                sysCpuLoad, sysLoadAvg, jvmTotalMemory, jvmFreeMemory, sysTotalMemory, sysFreeMemory,
                sysSwapTotal, sysSwapUsed, bytesReceived, bytesSent, totalDiskSpace, usableDiskSpace,
                processUptime, sysProcessCount, sysThreadCount, jobMap, transMap);
    }

    static String buildDummyServerStatusXml(String serverName) {
        return buildServerStatusXml(serverName, false, 0L, 0, 0, 0, 0.0D, 0.0D, 0L, 0L, 0L, 0L, 0L, 0L,
                0L, 0L, 0L, 0L, 0L, 0, 0, null, null);
    }

    static String buildServerStatusXml(String serverName, boolean isMaster,
                                       long sysUptime, int jvmCores,
                                       int sysPhysicalCores, int sysLogicalCores,
                                       double sysCpuLoad, double sysLoadAvg,
                                       long jvmTotalMemory, long jvmFreeMemory,
                                       long sysTotalMemory, long sysFreeMemory,
                                       long sysSwapTotal, long sysSwapUsed,
                                       long bytesReceived, long bytesSent,
                                       long totalDiskSpace, long usableDiskSpace,
                                       long processUptime, int sysProcessCount,
                                       int sysThreadCount,
                                       JobMap jobMap, TransformationMap transMap) {
        long startTime = System.currentTimeMillis();

        StringBuilder xml = new StringBuilder();

        xml.append(TAG_SERVER_STATUS_BEGIN)
                .append(XMLHandler.addTagValue(TAG_SERVER_NAME, serverName == null ? DEFAULT_SERVER_NAME : serverName))
                .append(XMLHandler.addTagValue(TAG_SERVER_TYPE, isMaster ? MASTER_SERVER : SLAVE_SERVER))
                .append(XMLHandler.addTagValue(TAG_SYS_UP_TIME, sysUptime < 0L ? 0L : sysUptime))
                .append(XMLHandler.addTagValue(TAG_PROC_UP_TIME, processUptime < 0L ? 0L : processUptime))
                .append(XMLHandler.addTagValue(TAG_JVM_CORES, jvmCores < 0 ? 0 : jvmCores))
                //.append(XMLHandler.addTagValue(TAG_SYS_PHYSICAL_CORES, sysPhysicalCores < 0 ? 0 : sysPhysicalCores))
                //.append(XMLHandler.addTagValue(TAG_SYS_LOGICAL_CORES, sysLogicalCores < 0 ? 0 : sysLogicalCores))
                .append(XMLHandler.addTagValue(TAG_SYS_CPU_LOAD, sysCpuLoad < 0.0D ? 0.0D : sysCpuLoad))
                .append(XMLHandler.addTagValue(TAG_SYS_LOAD_AVG, sysLoadAvg < 0.0D ? 0.0D : sysLoadAvg))
                .append(XMLHandler.addTagValue(TAG_JVM_MEM_TOTAL, jvmTotalMemory < 0L ? 0L : jvmTotalMemory))
                .append(XMLHandler.addTagValue(TAG_JVM_MEM_FREE, jvmFreeMemory < 0L ? 0L : jvmFreeMemory))
                .append(XMLHandler.addTagValue(TAG_SYS_MEM_TOTAL, sysTotalMemory < 0L ? 0L : sysTotalMemory))
                .append(XMLHandler.addTagValue(TAG_SYS_MEM_FREE, sysFreeMemory < 0L ? 0L : sysFreeMemory))
                .append(XMLHandler.addTagValue(TAG_SYS_SWAP_TOTAL, sysSwapTotal < 0L ? 0L : sysSwapTotal))
                .append(XMLHandler.addTagValue(TAG_SYS_SWAP_USED, sysSwapUsed < 0L ? 0L : sysSwapUsed))
                .append(XMLHandler.addTagValue(TAG_BYTE_RECEIVED, bytesReceived < 0L ? 0L : bytesReceived))
                .append(XMLHandler.addTagValue(TAG_BYTE_SENT, bytesSent < 0L ? 0L : bytesSent))
                .append(XMLHandler.addTagValue(TAG_SYS_DISK_TOTAL, totalDiskSpace < 0L ? 0L : totalDiskSpace))
                .append(XMLHandler.addTagValue(TAG_SYS_DISK_FREE, usableDiskSpace < 0L ? 0L : usableDiskSpace))
                .append(XMLHandler.addTagValue(TAG_SYS_PROCESSES, sysProcessCount < 0 ? 0 : sysProcessCount))
                .append(XMLHandler.addTagValue(TAG_SYS_THREADS, sysThreadCount < 0 ? 0 : sysThreadCount));

        int totalJobCount = 0;
        int uniqueJobCount = 0;
        int runningJobCount = 0;
        int stoppedJobCount = 0;
        int failedJobCount = 0;
        int finishedJobCount = 0;
        int haltedJobCount = 0;
        if (jobMap != null) {
            Set<String> jobsNames = new HashSet<>();
            for (CarteObjectEntry obj : jobMap.getJobObjects()) {
                Job job = jobMap.getJob(obj);
                totalJobCount++;

                boolean foundJobName = false;
                try {
                    for (String pName : JOB_NAME_PARAMS) {
                        String realName = job.getParameterValue(pName);

                        if (realName != null) {
                            foundJobName = true;
                            jobsNames.add(realName);
                            break;
                        }
                    }
                } catch (Exception e) {
                }
                if (!foundJobName) {
                    jobsNames.add(job.getName());
                }

                if (job.isActive() || !job.isInitialized()) {
                    if (job.isStopped()) {
                        haltedJobCount++;
                    } else {
                        runningJobCount++;
                    }
                } else {
                    if (job.isStopped()) {
                        stoppedJobCount++;
                    } else {
                        if (job.getErrors() > 0) {
                            failedJobCount++;
                        } else {
                            finishedJobCount++;
                        }
                    }
                }
            }
            uniqueJobCount = jobsNames.size();
        }
        xml.append(XMLHandler.addTagValue(TAG_TOTAL_JOBS, totalJobCount))
                .append(XMLHandler.addTagValue(TAG_UNIQUE_JOBS, uniqueJobCount))
                .append(XMLHandler.addTagValue(TAG_RUNNING_JOBS, runningJobCount))
                .append(XMLHandler.addTagValue(TAG_STOPPED_JOBS, stoppedJobCount))
                .append(XMLHandler.addTagValue(TAG_FAILED_JOBS, failedJobCount))
                .append(XMLHandler.addTagValue(TAG_FINISHED_JOBS, finishedJobCount))
                .append(XMLHandler.addTagValue(TAG_HALTED_JOBS, haltedJobCount));

        int totalTransCount = 0;
        int runningTransCount = 0;
        int haltedTransCount = 0;
        int stoppedTransCount = 0;
        int failedTransCount = 0;
        int finishedTransCount = 0;
        int pausedTransCount = 0;
        if (transMap != null) {
            for (CarteObjectEntry obj : transMap.getTransformationObjects()) {
                Trans trans = transMap.getTransformation(obj);
                totalTransCount++;

                if (trans.isRunning() || trans.isPreparing() || trans.isInitializing()) {
                    if (trans.isStopped()) {
                        haltedTransCount++;
                    } else {
                        if (trans.isFinished()) {
                            if (trans.getErrors() > 0) {
                                failedTransCount++;
                            } else {
                                finishedTransCount++;
                            }
                        } else if (trans.isPaused()) {
                            pausedTransCount++;
                        } else {
                            runningTransCount++;
                        }
                    }
                } else if (trans.isStopped()) {
                    stoppedTransCount++;
                } else { // treat waiting as finished
                    finishedTransCount++;
                }
            }
        }
        xml.append(XMLHandler.addTagValue(TAG_TOTAL_TRANS, totalTransCount))
                .append(XMLHandler.addTagValue(TAG_RUNNING_TRANS, runningTransCount))
                .append(XMLHandler.addTagValue(TAG_PAUSED_TRANS, pausedTransCount))
                .append(XMLHandler.addTagValue(TAG_STOPPED_TRANS, stoppedTransCount))
                .append(XMLHandler.addTagValue(TAG_FAILED_TRANS, failedTransCount))
                .append(XMLHandler.addTagValue(TAG_FINISHED_TRANS, finishedTransCount))
                .append(XMLHandler.addTagValue(TAG_HALTED_TRANS, haltedTransCount));

        // xml.append(XMLHandler.addTagValue(TAG_ELAPSED_TIME, System.currentTimeMillis() - startTime));
        xml.append(TAG_SERVER_STATUS_END);

        return xml.toString();
    }

    static String getCurrentServerName(SlaveServer server) {
        if (SERVER_NAME == null) {
            String serverName = getServerName(server);
            if (serverName == null || server == null || server.getHostname() == null) {
                try {
                    serverName = InetAddress.getLocalHost().getHostName();
                } catch (Exception e) {
                    // it's just about getting host name, so let's pretend nothing happened...
                }
            }

            SERVER_NAME = serverName;
        }

        return SERVER_NAME;
    }

    static String getServerName(SlaveServer server) {
        return server == null ? null : server.getServerAndPort();
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

        response.setContentType(XML_CONTENT_TYPE);
        response.setCharacterEncoding(Const.XML_ENCODING);

        SlaveServer currentServer = CarteSingleton.getSlaveServerConfig().getSlaveServer();
        String currentServerName = getCurrentServerName(currentServer);

        PrintWriter out = response.getWriter();

        boolean partialXml = PARAM_PARTIAL_YES.equalsIgnoreCase(request.getParameter(PARAM_PARTIAL));

        if (!partialXml) {
            out.print(XMLHandler.getXMLHeader(Const.XML_ENCODING));

            StringBuilder xml = new StringBuilder();
            xml.append(TAG_SERVER_STATUS_LIST_BEGIN).append(buildServerHealthXml(currentServerName,
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
                                server.execService(SLAVE_CONTEXT_PATH));
                    } catch (Exception e) {
                        xml.append(buildDummyServerStatusXml(getServerName(server)));
                    }
                }
            }

            xml.append(TAG_SERVER_STATUS_LIST_END);
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
