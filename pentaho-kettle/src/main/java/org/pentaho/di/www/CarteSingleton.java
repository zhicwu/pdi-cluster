/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2016 by Pentaho : http://www.pentaho.com
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

import org.pentaho.di.cluster.ServerCache;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.*;
import org.pentaho.di.core.util.EnvUtil;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.Job;
import org.pentaho.di.trans.Trans;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class CarteSingleton {

    private static Class<?> PKG = Carte.class; // for i18n purposes, needed by Translator2!!

    private static SlaveServerConfig slaveServerConfig;
    private static CarteSingleton carteSingleton;
    private static Carte carte;

    private LogChannelInterface log;

    private TransformationMap transformationMap;
    private JobMap jobMap;
    private List<SlaveServerDetection> detections;
    private SocketRepository socketRepository;

    private CarteSingleton(SlaveServerConfig config) throws KettleException {
        KettleEnvironment.init();
        KettleLogStore.init(config.getMaxLogLines(), config.getMaxLogTimeoutMinutes());

        this.log = new LogChannel("Carte");
        transformationMap = new TransformationMap();
        transformationMap.setSlaveServerConfig(config);
        jobMap = new JobMap();
        jobMap.setSlaveServerConfig(config);
        detections = new ArrayList<SlaveServerDetection>();
        socketRepository = new SocketRepository(log);

        installPurgeTimer(config, log, transformationMap, jobMap);

        SlaveServer slaveServer = config.getSlaveServer();
        if (slaveServer != null) {
            int port = WebServer.PORT;
            if (!Utils.isEmpty(slaveServer.getPort())) {
                try {
                    port = Integer.parseInt(slaveServer.getPort());
                } catch (Exception e) {
                    log.logError(BaseMessages.getString(PKG, "Carte.Error.CanNotPartPort", slaveServer.getHostname(), ""
                            + port), e);
                }
            }

            // TODO: see if we need to keep doing this on a periodic basis.
            // The master might be dead or not alive yet at the time we send this
            // message.
            // Repeating the registration over and over every few minutes might
            // harden this sort of problems.
            //

            /* sorry MasterDetector will take care of the following
            if ( config.isReportingToMasters() ) {
                String hostname = slaveServer.getHostname();
                final SlaveServer client =
                        new SlaveServer( "Dynamic slave [" + hostname + ":" + port + "]", hostname, "" + port, slaveServer
                                .getUsername(), slaveServer.getPassword() );
                for ( final SlaveServer master : config.getMasters() ) {
                    // Here we use the username/password specified in the slave
                    // server section of the configuration.
                    // This doesn't have to be the same pair as the one used on the
                    // master!
                    //
                    try {
                        SlaveServerDetection slaveServerDetection = new SlaveServerDetection( client );
                        master.sendXML( slaveServerDetection.getXML(), RegisterSlaveServlet.CONTEXT_PATH + "/" );
                        log.logBasic( "Registered this slave server to master slave server ["
                                + master.toString() + "] on address [" + master.getServerAndPort() + "]" );
                    } catch ( Exception e ) {
                        log.logError( "Unable to register to master slave server ["
                                + master.toString() + "] on address [" + master.getServerAndPort() + "]" );
                    }
                }
            }
            */
        }
    }

    public static void installPurgeTimer(final SlaveServerConfig config, final LogChannelInterface log,
                                         final TransformationMap transformationMap, final JobMap jobMap) {

        final int objectTimeout;
        String systemTimeout = EnvUtil.getSystemProperty(Const.KETTLE_CARTE_OBJECT_TIMEOUT_MINUTES, null);

        // The value specified in XML takes precedence over the environment variable!
        //
        if (config.getObjectTimeoutMinutes() > 0) {
            objectTimeout = config.getObjectTimeoutMinutes();
        } else if (!Utils.isEmpty(systemTimeout)) {
            objectTimeout = Const.toInt(systemTimeout, 1440);
        } else {
            objectTimeout = 24 * 60; // 1440 : default is a one day time-out
        }

        if (!ServerCache.RESOURCE_CACHE_DISABLED && objectTimeout <= ServerCache.RESOURCE_EXPIRATION_MINUTE) {
            log.logBasic(new StringBuilder().append("You may want to increase ")
                    .append(Const.KETTLE_CARTE_OBJECT_TIMEOUT_MINUTES).append(" from ")
                    .append(objectTimeout).append(" minutes to ")
                    .append(ServerCache.RESOURCE_EXPIRATION_MINUTE + 1)
                    .append(" to fully utilize resource cache.").toString());
        }

        // If we need to time out finished or idle objects, we should create a timer
        // in the background to clean
        //
        if (objectTimeout > 0) {

            log.logBasic("Installing timer to purge stale objects after " + objectTimeout + " minutes.");

            Timer timer = new Timer("Timer-PurgeStaleObject", true);

            final AtomicBoolean busy = new AtomicBoolean(false);
            TimerTask timerTask = new TimerTask() {
                public void run() {
                    if (busy.compareAndSet(false, true)) {
                        try {
                            // Check all transformations...
                            //
                            for (CarteObjectEntry entry : transformationMap.getTransformationObjects()) {
                                Trans trans = transformationMap.getTransformation(entry);
                                Date logDate = trans.getLogDate();

                                // See if the transformation is finished or stopped.
                                //
                                if (trans != null && (trans.isFinished() || trans.isStopped()) && logDate != null) {
                                    // check the last log time
                                    //
                                    int diffInMinutes =
                                            (int) Math.floor((System.currentTimeMillis() - logDate.getTime()) / 60000);
                                    if (diffInMinutes >= objectTimeout) {
                                        String logChannelId = trans.getLogChannelId();

                                        // Let's remove this from the transformation map...
                                        //
                                        transformationMap.removeTransformation(entry);

                                        // Remove the logging information from the log registry & central log store
                                        //
                                        KettleLogStore.discardLines(logChannelId, false);
                                        LoggingRegistry.getInstance().removeIncludingChildren(logChannelId);

                                        // transformationMap.deallocateServerSocketPorts(entry);

                                        log.logMinimal("Cleaned up transformation "
                                                + entry.getName() + " with id " + entry.getId() + " from " + logDate
                                                + ", diff=" + diffInMinutes);
                                    }
                                }
                            }

                            // And the jobs...
                            //
                            for (CarteObjectEntry entry : jobMap.getJobObjects()) {
                                Job job = jobMap.getJob(entry);
                                Date logDate = job.getLogDate();

                                // See if the job is finished or stopped.
                                //
                                if (job != null && (job.isFinished() || job.isStopped()) && logDate != null) {
                                    // check the last log time
                                    //
                                    int diffInMinutes =
                                            (int) Math.floor((System.currentTimeMillis() - logDate.getTime()) / 60000);
                                    if (diffInMinutes >= objectTimeout) {
                                        String logChannelId = job.getLogChannelId();

                                        // Let's remove this from the job map...
                                        //
                                        jobMap.removeJob(entry);

                                        // Remove the logging information from the log registry & central log store
                                        //
                                        KettleLogStore.discardLines(logChannelId, false);
                                        LoggingRegistry.getInstance().removeIncludingChildren(logChannelId);

                                        log.logMinimal("Cleaned up job "
                                                + entry.getName() + " with id " + entry.getId() + " from " + logDate);
                                    }
                                }
                            }

                        } catch (Throwable t) {
                            log.logError("Error occurred while purging stable objects", t);
                        } finally {
                            busy.set(false);
                        }
                    }
                }
            };

            // Search for stale objects every 20 seconds:
            //
            timer.schedule(timerTask, 20000, 20000);
        }
    }

    public static CarteSingleton getInstance() {
        try {
            if (carteSingleton == null) {
                if (slaveServerConfig == null) {
                    slaveServerConfig = new SlaveServerConfig();
                    SlaveServer slaveServer = new SlaveServer();
                    slaveServerConfig.setSlaveServer(slaveServer);
                }

                carteSingleton = new CarteSingleton(slaveServerConfig);

                String carteObjectId = UUID.randomUUID().toString();
                SimpleLoggingObject servletLoggingObject =
                        new SimpleLoggingObject("CarteSingleton", LoggingObjectType.CARTE, null);
                servletLoggingObject.setContainerObjectId(carteObjectId);
                servletLoggingObject.setLogLevel(LogLevel.BASIC);

                return carteSingleton;
            } else {
                return carteSingleton;
            }
        } catch (KettleException ke) {
            throw new RuntimeException(ke);
        }
    }

    public TransformationMap getTransformationMap() {
        return transformationMap;
    }

    public void setTransformationMap(TransformationMap transformationMap) {
        this.transformationMap = transformationMap;
    }

    public JobMap getJobMap() {
        return jobMap;
    }

    public void setJobMap(JobMap jobMap) {
        this.jobMap = jobMap;
    }

    public List<SlaveServerDetection> getDetections() {
        return detections;
    }

    public void setDetections(List<SlaveServerDetection> detections) {
        this.detections = detections;
    }

    public SocketRepository getSocketRepository() {
        return socketRepository;
    }

    public void setSocketRepository(SocketRepository socketRepository) {
        this.socketRepository = socketRepository;
    }

    public static SlaveServerConfig getSlaveServerConfig() {
        return slaveServerConfig;
    }

    public static void setSlaveServerConfig(SlaveServerConfig slaveServerConfig) {
        CarteSingleton.slaveServerConfig = slaveServerConfig;
    }

    public static void setCarte(Carte carte) {
        CarteSingleton.carte = carte;
    }

    public static Carte getCarte() {
        return CarteSingleton.carte;
    }

    public LogChannelInterface getLog() {
        return log;
    }
}
