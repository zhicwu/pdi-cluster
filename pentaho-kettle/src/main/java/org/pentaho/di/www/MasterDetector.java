/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2013 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
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

import org.apache.commons.lang.StringUtils;
import org.pentaho.database.model.DatabaseConnection;
import org.pentaho.database.model.IDatabaseConnection;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.util.DataSourceLocator;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.util.EnvUtil;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.io.StringReader;
import java.util.*;

/**
 * This class is responsible for communicating with master node, BA server in this case.
 * It helps to re-register slave or re-import data sources as required(i.e. master restarted).
 *
 * @author Zhichun Wu
 */
final class MasterDetector implements Runnable {
    static final class MasterServerStatus {
        boolean alive = false;
        boolean restarted = false;
        long uptime = 0L;
        String dslist = EMPTY_STRING;
    }

    final static String PROP_INITIAL_DELAY = "KETTLE_MASTER_DETECTOR_INITIAL_DELAY";
    final static String PROP_REFRESH_INTERVAL = "KETTLE_MASTER_DETECTOR_REFRESH_INTERVAL";

    final static long DEFAULT_INITIAL_DELAY = 1 * 1000L; // 1 seconds
    final static long DEFAULT_REFRESH_INTERVAL = 10 * 1000L; // 10 seconds

    final static String PATH_QUERY_STATUS = "/kettle/status?xml=Y";
    final static String PATH_QUERY_SLAVES = "/kettle/getSlaves?xml=Y";
    // this is not build-in API like above in Kettle but comes from BA server
    final static String PATH_QUERY_DS = "/plugin/data-access/api/datasource/jdbc/connection";

    final static String TAG_UPTIME_BEGIN = "<uptime>";
    final static String TAG_UPTIME_END = "</uptime>";

    final static String TAG_HOST_BEGIN = "<hostname>";
    final static String TAG_HOST_END = "</hostname>";
    final static String TAG_PORT_BEGIN = "<port>";
    final static String TAG_PORT_END = "</port>";

    final static String EMPTY_STRING = "";

    final static MasterDetector instance = new MasterDetector();

    private final long initialDelay;
    private final long refreshInterval;
    private final JAXBContext jaxbContext;
    private final Map<String, MasterServerStatus> masterStatus
            = Collections.synchronizedMap(new HashMap<String, MasterServerStatus>(3));

    private MasterDetector() {
        JAXBContext context = null;
        try {
            context = JAXBContext.newInstance(JaxbList.class, DatabaseConnection.class);
        } catch (JAXBException e) {
            e.printStackTrace();
        } finally {
            jaxbContext = context;
        }

        this.initialDelay = Const.toLong(EnvUtil.getSystemProperty(PROP_INITIAL_DELAY), DEFAULT_INITIAL_DELAY);
        this.refreshInterval = Const.toLong(EnvUtil.getSystemProperty(PROP_REFRESH_INTERVAL), DEFAULT_REFRESH_INTERVAL);
    }

    private void checkConfig() {
        Carte carte = CarteSingleton.getCarte();
        SlaveServerConfig config = carte == null ? null : carte.getConfig();
        List<SlaveServer> masters = config == null ? null : config.getMasters();
        if (config == null || masters == null || masters.size() == 0) {
            throw new NullPointerException("At least one master is required for the cluster");
        }

        if (!config.isReportingToMasters()) {
            throw new IllegalStateException("Only slave server can register on master nodes");
        }
    }

    private void checkMasterRegistrion() {
        checkConfig();

        SlaveServerConfig config = CarteSingleton.getCarte().getConfig();
        SlaveServer slaveServer = config.getSlaveServer();

        LogChannelInterface log = CarteSingleton.getInstance().getLog();

        for (final SlaveServer master : config.getMasters()) {
            String name = master.getName();
            MasterServerStatus status = masterStatus.get(name);

            if (status == null || !status.alive) {
                // this should never happen
                log.logError(new StringBuilder().append("Skip master ")
                        .append(name).append(" as it seems dead").toString());
                continue;
            }

            String tag = new StringBuilder().append(TAG_HOST_BEGIN)
                    .append(slaveServer.getHostname()).append(TAG_HOST_END).append(TAG_PORT_BEGIN)
                    .append(slaveServer.getPort()).append(TAG_PORT_END).toString();

            try {
                String xml = master.sendXML(EMPTY_STRING, PATH_QUERY_SLAVES);
                if (xml != null && (status.restarted || xml.indexOf(tag) < 0)) {
                    registerOnMasters(master);
                }
            } catch (Throwable t) {
                log.logError("Failed to check slaves of master " + name + ": " + t.getMessage());
                status.alive = false;
                continue;
            }

            // now check data sources
            Map<String, String> headerValues = new HashMap<String, String>(1);
            headerValues.put("Accept", "application/xml");
            String dslist = EMPTY_STRING;
            try {
                // no retry
                dslist = master.execService(PATH_QUERY_DS, headerValues);
                if (dslist != null && (!status.dslist.equals(dslist) || status.restarted)) {
                    Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
                    JaxbList<String> list = (JaxbList<String>) jaxbUnmarshaller.unmarshal(new StringReader(dslist));
                    Map<String, IDatabaseConnection> mapping
                            = new HashMap<String, IDatabaseConnection>(list.getList().size());
                    for (String s : list.getList()) {
                        String ds = master.execService(
                                new StringBuilder().append(PATH_QUERY_DS).append('/').append(s).toString(),
                                headerValues);
                        mapping.put(s, (IDatabaseConnection) jaxbUnmarshaller.unmarshal(new StringReader(ds)));
                    }

                    log.logBasic(
                            new StringBuilder().append(DataSourceLocator.updateDataSourceMappings(mapping))
                                    .append(" of ").append(mapping.size()).append(" data sources imported from master ")
                                    .append(master.getName()).toString());

                    status.dslist = dslist;
                }
                // log.logBasic(xml);
            } catch (Throwable t) {
                log.logError("Failed to check data sources of master " + name + ": " + t.getMessage());
                // status.alive = false;
            }
        }
    }

    private void checkMasterStatus() {
        checkConfig();

        SlaveServerConfig config = CarteSingleton.getCarte().getConfig();
        LogChannelInterface log = CarteSingleton.getInstance().getLog();

        for (final SlaveServer master : config.getMasters()) {
            String name = master.getName();
            MasterServerStatus status = masterStatus.get(name);
            if (status == null) {
                status = new MasterServerStatus();
                masterStatus.put(name, status);
            }

            int startIndex = 0;
            long uptime = 0L;
            try {
                String xml = master.sendXML(EMPTY_STRING, PATH_QUERY_STATUS);

                startIndex = xml == null ? -1 : xml.indexOf(TAG_UPTIME_BEGIN);

                if (startIndex > 0) {
                    startIndex = startIndex + TAG_UPTIME_BEGIN.length();
                    int endIndex = xml.indexOf(TAG_UPTIME_END, startIndex);
                    if (endIndex > startIndex) {
                        uptime = Long.parseLong(xml.substring(startIndex, endIndex));
                    }
                }

                log.logDebug(new StringBuilder().append(name).append(':').append(uptime)
                        .append('(').append(uptime > status.uptime).append(')').toString());
            } catch (Throwable t) {
                log.logError("Failed to get status of master " + name + ": " + t.getMessage());
            } finally {
                status.alive = startIndex > 0;
                status.restarted = uptime > 0L && status.uptime > 0L && uptime < status.uptime;
                if (uptime > 0L) {
                    status.uptime = uptime;
                }
            }
        }
    }

    long getInitialDelay() {
        return this.initialDelay;
    }

    long getRefreshInterval() {
        return this.refreshInterval;
    }

    boolean registerOnMasters(SlaveServer... masters) {
        checkConfig();

        boolean allOK = true;

        SlaveServerConfig config = CarteSingleton.getCarte().getConfig();
        SlaveServer slaveServer = config.getSlaveServer();
        masters = masters == null || masters.length == 0
                ? config.getMasters().toArray(new SlaveServer[config.getMasters().size()]) : masters;

        LogChannelInterface log = CarteSingleton.getInstance().getLog();

        Properties masterProperties = null;
        String propertiesMaster = slaveServer.getPropertiesMasterName();
        for (final SlaveServer master : masters) {
            // Here we use the username/password specified in the slave server section of the configuration.
            // This doesn't have to be the same pair as the one used on the master!
            //
            try {
                SlaveServerDetection slaveServerDetection = new SlaveServerDetection(slaveServer.getClient());
                master.sendXML(slaveServerDetection.getXML(), RegisterSlaveServlet.CONTEXT_PATH + "/");
                log.logBasic("Registered this slave server to master slave server [" + master.toString() + "] on address ["
                        + master.getServerAndPort() + "]");
            } catch (Exception e) {
                log.logError("Unable to register to master slave server [" + master.toString() + "] on address [" + master
                        .getServerAndPort() + "]");
                allOK = false;
            }
            try {
                if (!StringUtils.isBlank(propertiesMaster) && propertiesMaster.equalsIgnoreCase(master.getName())) {
                    if (masterProperties != null) {
                        log.logError("More than one primary master server. Master name is " + propertiesMaster);
                    } else {
                        masterProperties = master.getKettleProperties();
                        log.logBasic("Got properties from master server [" + master.toString() + "], address [" + master
                                .getServerAndPort() + "]");
                    }
                }
            } catch (Exception e) {
                log.logError("Unable to get properties from master server [" + master.toString() + "], address [" + master
                        .getServerAndPort() + "]");
                allOK = false;
            }
        }

        if (masterProperties != null) {
            EnvUtil.applyKettleProperties(masterProperties, slaveServer.isOverrideExistingProperties());
        }

        return allOK;
    }

    @Override
    public void run() {
        try {
            // first check if the masters are alive(and their uptime) - yes, we may have more than one master
            checkMasterStatus();

            // and then check if this slave has been registered in all these masters
            // in case it's not(ex: master restarted), register the salve again
            // lastly, re-import data sources if there's any change
            checkMasterRegistrion();
        } catch (Exception e) {
            // do NOT throw exception here or the scheduled task will stop running
        }
    }
}
