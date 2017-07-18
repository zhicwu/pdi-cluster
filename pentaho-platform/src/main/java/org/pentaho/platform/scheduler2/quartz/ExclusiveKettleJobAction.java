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
package org.pentaho.platform.scheduler2.quartz;

import com.google.common.base.Splitter;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pentaho.di.www.CarteObjectEntry;
import org.pentaho.di.www.CarteSingleton;
import org.pentaho.di.www.JobMap;
import org.quartz.JobExecutionException;

import java.util.*;
import java.util.concurrent.Callable;

import static org.pentaho.platform.scheduler2.quartz.QuartzSchedulerHelper.*;

/**
 * This class represents possible action supported by ExclusiveKettleJobRule.
 */
public class ExclusiveKettleJobAction {
    private static final Log logger = LogFactory.getLog(ExclusiveKettleJobAction.class);

    private static final Splitter PARAM_SPLITTER = Splitter.on('(').trimResults();
    private static final Splitter ACTION_SPLITTER = Splitter.on(';').omitEmptyStrings().trimResults();
    private static final Splitter KVP_SPLITTER = Splitter.on('=').trimResults();
    private static final Splitter JOB_SPLITTER = Splitter.on(',').omitEmptyStrings().trimResults();
    private static final Splitter KEY_SPLITTER = Splitter.on('\t').trimResults();

    private static final Cache<String, long[]> jobCache
            = CacheBuilder.newBuilder().maximumSize(KETTLE_JOB_CACHE_SIZE).build();

    private static long increaseJobOccurrence(String jobId) {
        final long currentTimestamp = System.currentTimeMillis();

        long[] occurrence = null;
        try {
            occurrence = jobCache.get(jobId, new Callable<long[]>() {
                @Override
                public long[] call() throws Exception {
                    return new long[]{currentTimestamp - KETTLE_JOB_OCCURRENCE_INTERVAL, 0L};
                }
            });
        } catch (Exception e) {
            // Either checked or unchecked exception will never happen
            occurrence = new long[]{currentTimestamp - KETTLE_JOB_OCCURRENCE_INTERVAL, 0L};
        }

        long diff = currentTimestamp - occurrence[0];
        long counter = occurrence[1];
        if (diff >= KETTLE_JOB_MAX_DURATION) {
            logger.warn(new StringBuffer()
                    .append("Decided to kill job [")
                    .append(jobId)
                    .append("] as it's running for too long (>=")
                    .append(KETTLE_JOB_MAX_DURATION)
                    .append("ms)").toString());
            counter = KETTLE_JOB_MAX_OCCURRENCE;
        } else if (diff >= KETTLE_JOB_OCCURRENCE_INTERVAL) {
            jobCache.put(jobId, new long[]{currentTimestamp, ++counter});
        } else {
            logger.debug(new StringBuffer()
                    .append("Ignore this occurrence of [")
                    .append(jobId)
                    .append("] to avoid double count within ")
                    .append(KETTLE_JOB_OCCURRENCE_INTERVAL)
                    .append("ms").toString());
            counter = -1;
        }

        return counter;
    }

    private static void removeJobFromCache(String jobId) {
        jobCache.invalidate(jobId);
    }

    public enum ActionType {
        /**
         * This is the default action which does nothing.
         */
        DONOTHING,
        /**
         * Respect given jobs by stopping current job.
         * <p>
         * Usage: Exclusive(RESPECT=Job1,Job2,Job3...)
         */
        RESPECT,

        /**
         * Kill given jobs before running current one.
         * <p>
         * Usage: Exclusive(KILL=Job1,Job2,Job3...)
         */
        KILL;
    }

    /**
     * Parse the given action string.
     *
     * @param jobKey          key of quartz job
     * @param executionPolicy action string, for example: "Exclusive(Kill=Job1,Job2;Respect=Job3)"
     * @return parsed action list
     */
    public static List<ExclusiveKettleJobAction> extractActions(QuartzJobKey jobKey, String executionPolicy) {
        List<ExclusiveKettleJobAction> actions = new ArrayList<>();

        if (executionPolicy != null && executionPolicy.startsWith(EXEC_POLICY_EXCLUSIVE)) {
            String action = ActionType.DONOTHING.name();
            List<String> parts = PARAM_SPLITTER.splitToList(executionPolicy);
            if (parts.size() == 2) {
                action = parts.get(1);
                if (action.length() > 0 && action.charAt(action.length() - 1) == ')') {
                    action = action.substring(0, action.length() - 1);
                }
            }

            Map<String, ExclusiveKettleJobAction> cache = new HashMap<>();
            for (String str : ACTION_SPLITTER.split(action)) {
                List<String> kvp = KVP_SPLITTER.splitToList(str);
                if (kvp.size() == 2) {
                    String key = kvp.get(0).toLowerCase();
                    ExclusiveKettleJobAction ea = cache.get(key);
                    if (ea == null) {
                        ea = new ExclusiveKettleJobAction(jobKey, key, kvp.get(1));
                        cache.put(key, ea);
                        actions.add(ea);
                    } else {
                        ea.jobNames.addAll(JOB_SPLITTER.splitToList(kvp.get(1)));
                    }
                }
            }

            if (!cache.containsKey(ActionType.RESPECT.name().toLowerCase())) {
                actions.add(new ExclusiveKettleJobAction(jobKey, ActionType.RESPECT.name(), jobKey.getJobName()));
            }
        }

        return actions;
    }

    private final QuartzJobKey jobKey;
    private final ActionType actionType;
    private final Set<String> jobNames;

    private ExclusiveKettleJobAction(QuartzJobKey jobKey, String action, String jobs) {
        this.jobKey = jobKey;

        ActionType type = ActionType.DONOTHING;
        for (ActionType each : ActionType.class.getEnumConstants()) {
            if (each.name().compareToIgnoreCase(action) == 0) {
                type = each;
                break;
            }
        }
        this.actionType = type;

        List<String> names = JOB_SPLITTER.splitToList(jobs);
        this.jobNames = new HashSet<>(names.size());
        this.jobNames.addAll(names);
    }

    private String extractJobName(String name) {
        if (name != null) {
            List<String> keys = KEY_SPLITTER.splitToList(name);
            // <user name>\t<job name>\t<timestamp>
            // FIXME what if the job is scheduled by a non-admin user?
            if (keys.size() == 3) {
                name = keys.get(1);
            }
        }

        return name;
    }

    private void stopJob(String carteObjId, String jobName, org.pentaho.di.job.Job job)
            throws InterruptedException, JobExecutionException {
        job.stopAll();
        // check every 2 seconds until the job is no longer active, time out after 8 seconds
        long timeLimit = System.currentTimeMillis() + KETTLE_JOB_KILLER_MAX_WAIT;
        boolean timedout = true;
        while (System.currentTimeMillis() < timeLimit) {
            Thread.sleep(KETTLE_JOB_KILLER_CHECK_INTERVAL);
            if (!job.isActive()) {
                logger.warn(new StringBuilder()
                        .append("Successfully killed [")
                        .append(jobName)
                        .append('(')
                        .append(carteObjId)
                        .append(")] within ~")
                        .append((timeLimit - System.currentTimeMillis()) / 1000)
                        .append(" seconds, before running exclusive job [")
                        .append(jobKey)
                        .append(']').toString());
                timedout = false;
                break;
            }
        }

        if (timedout) {
            removeJobFromCache(carteObjId);
            throw new JobExecutionException(new StringBuilder()
                    .append("Stop exclusive job [")
                    .append(jobKey)
                    .append("] as it took too long( >= ")
                    .append(KETTLE_JOB_KILLER_MAX_WAIT / 1000)
                    .append(" seconds) to kill [")
                    .append(jobName)
                    .append('(')
                    .append(carteObjId)
                    .append(")]")
                    .toString());
        }
    }

    public void execute(Phase phase) throws JobExecutionException {
        if (actionType == ActionType.DONOTHING) {
            return;
        }

        // Let's not worry about transformation for now as in general job is better on error handling than trans
        /*
        TransformationMap transMap = CarteSingleton.getInstance().getTransformationMap();
        for(CarteObjectEntry carteObj : transMap.getTransformationObjects()) {
            Trans trans = transMap.getTransformation(carteObj);
            if (!trans.isFinished() && !trans.isStopped()) {
                trans.stopAll();
                // check every 1s to see if the job has been stopped, time out after 5 seconds
            }
        }
        */

        JobMap jobMap = CarteSingleton.getInstance().getJobMap();
        String currentJobName = jobKey.getJobName();
        for (CarteObjectEntry carteObj : jobMap.getJobObjects()) {
            try {
                String jobId = carteObj.getId();
                org.pentaho.di.job.Job job = jobMap.getJob(carteObj);
                if (job.isActive() && !job.isStopped()) {
                    String jobName = extractJobName(job.getParameterValue(KEY_ETL_JOB_ID));

                    // kill the job instance on the consecutive 3rd time we met it
                    if (actionType == ActionType.RESPECT && currentJobName.equals(jobName)) {
                        long occurrence = increaseJobOccurrence(jobId);
                        if (occurrence < 0) {
                            continue;
                        }

                        logger.warn(new StringBuilder()
                                .append("Counting down the occurrence of ")
                                .append(jobName).append('[').append(jobId).append("]: ")
                                .append(KETTLE_JOB_MAX_OCCURRENCE - occurrence + 1).toString());

                        if (occurrence >= KETTLE_JOB_MAX_OCCURRENCE) {
                            stopJob(jobId, jobName, job);
                        }
                    } else if (jobNames.contains(jobName)) {
                        if (actionType == ActionType.RESPECT) {
                            throw new JobExecutionException(new StringBuilder()
                                    .append("Discard exclusive job [")
                                    .append(jobKey)
                                    .append("] because [")
                                    .append(jobName)
                                    .append('(')
                                    .append(jobId)
                                    .append(")] is running")
                                    .toString());
                        } else if (actionType == ActionType.KILL) {
                            stopJob(jobId, jobName, job);
                        }
                    }
                } else {
                    removeJobFromCache(jobId);
                }
            } catch (JobExecutionException e) {
                throw e;
            } catch (Throwable t) {
                throw new JobExecutionException(new StringBuilder()
                        .append("Failed to kill job [")
                        .append(carteObj)
                        .append("] when running job [")
                        .append(jobKey)
                        .append(']')
                        .toString(), t);
            }
        }
    }
}
