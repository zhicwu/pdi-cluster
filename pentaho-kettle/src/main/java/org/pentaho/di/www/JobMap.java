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

import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobConfiguration;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * This is a map between the job name and the (running/waiting/finished) job.
 *
 * @author Matt
 * @since 3.0.0
 */
public class JobMap {
    private KettleTaskMap<Job, JobConfiguration> map;

    private SlaveServerConfig slaveServerConfig;

    private void initCache() {
        if (map != null) {
            map.clear();
        }

        map = new KettleTaskMap<>(slaveServerConfig);
    }

    public JobMap() {
        initCache();
    }

    public synchronized void addJob(String jobName, String carteObjectId, Job job, JobConfiguration jobConfiguration) {
        CarteObjectEntry entry = new CarteObjectEntry(jobName, carteObjectId);
        map.cache.put(entry, map.createEntry(job, jobConfiguration));
    }

    public synchronized void registerJob(Job job, JobConfiguration jobConfiguration) {
        job.setContainerObjectId(UUID.randomUUID().toString());
        CarteObjectEntry entry = new CarteObjectEntry(job.getJobMeta().getName(), job.getContainerObjectId());
        map.cache.put(entry, map.createEntry(job, jobConfiguration));
    }

    public synchronized void replaceJob(CarteObjectEntry entry, Job job, JobConfiguration jobConfiguration) {
        map.cache.put(entry, map.createEntry(job, jobConfiguration));
    }

    /**
     * Find the first job in the list that comes to mind!
     *
     * @param jobName
     * @return the first transformation with the specified name
     */
    public synchronized Job getJob(String jobName) {
        for (CarteObjectEntry entry : map.cache.asMap().keySet()) {
            if (entry.getName().equals(jobName)) {
                return getJob(entry);
            }
        }
        return null;
    }

    /**
     * @param entry The Carte job object
     * @return the job with the specified entry
     */
    public synchronized Job getJob(CarteObjectEntry entry) {
        KettleTaskMap.EntryInfo<Job, JobConfiguration> info = map.cache.getIfPresent(entry);

        return info == null ? null : info.entry;
    }

    public synchronized JobConfiguration getConfiguration(String jobName) {
        for (CarteObjectEntry entry : map.cache.asMap().keySet()) {
            if (entry.getName().equals(jobName)) {
                return getConfiguration(entry);
            }
        }
        return null;
    }

    /**
     * @param entry The Carte job object
     * @return the job configuration with the specified entry
     */
    public synchronized JobConfiguration getConfiguration(CarteObjectEntry entry) {
        KettleTaskMap.EntryInfo<Job, JobConfiguration> info = map.cache.getIfPresent(entry);

        return info == null ? null : info.config;
    }

    public synchronized void removeJob(CarteObjectEntry entry) {
        map.cache.invalidate(entry);
    }

    public synchronized List<CarteObjectEntry> getJobObjects() {
        return new ArrayList<>(map.cache.asMap().keySet());
    }

    public synchronized CarteObjectEntry getFirstCarteObjectEntry(String jobName) {
        for (CarteObjectEntry key : map.cache.asMap().keySet()) {
            if (key.getName().equals(jobName)) {
                return key;
            }
        }

        return null;
    }

    /**
     * @return the slaveServerConfig
     */
    public SlaveServerConfig getSlaveServerConfig() {
        return slaveServerConfig;
    }

    /**
     * @param slaveServerConfig the slaveServerConfig to set
     */
    public void setSlaveServerConfig(SlaveServerConfig slaveServerConfig) {
        if (this.slaveServerConfig != slaveServerConfig) {
            this.slaveServerConfig = slaveServerConfig;

            this.initCache();
        }
    }

    /**
     * Find a job using the container/carte object ID.
     *
     * @param id the container/carte object ID
     * @return The job if it's found, null if the ID couldn't be found in the job map.
     */
    public synchronized Job findJob(String id) {
        for (KettleTaskMap.EntryInfo<Job, JobConfiguration> info : map.cache.asMap().values()) {
            if (info.entry.getContainerObjectId().equals(id)) {
                return info.entry;
            }
        }

        return null;
    }

    String getStats() {
        return map.getStats();
    }
}
