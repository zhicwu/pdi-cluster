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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.quartz.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.pentaho.platform.scheduler2.quartz.QuartzSchedulerHelper.*;

/**
 * Define exclusive execution rule, which will be used in scheduling service.
 *
 * @author Zhichun Wu
 */
public class ExclusiveKettleJobRule { // implements TriggerListener {
    static final ExclusiveKettleJobRule instance = new ExclusiveKettleJobRule();

    private static final Log logger = LogFactory.getLog(ExclusiveKettleJobRule.class);

    /* It won't help even you implemented TriggerListener - you have to know which jobs are running
       and when did last trigger completed, regardless it's success or failed

    private static final String TRIGGER_LISTENER_NAME = "ExclusiveTriggerListener";

    private final Map<String, Date> jobFireDates = Collections.synchronizedMap(new HashMap<>());

    private static String getExclusiveJobName(JobExecutionContext jobExecutionContext) {
        Scheduler scheduler = jobExecutionContext.getScheduler();
        JobDetail jobDetail = jobExecutionContext.getJobDetail();

        QuartzJobKey jobKey = extractJobKey(jobDetail);

        if (scheduler != null && jobKey != null) {
            JobDataMap params = jobDetail.getJobDataMap();
            String actionId = params.getString(QuartzScheduler.RESERVEDMAPKEY_ACTIONID);
            Object streamProvider = params.get(QuartzScheduler.RESERVEDMAPKEY_STREAMPROVIDER);
            streamProvider = streamProvider == null ? null : streamProvider.toString();
            Object map = params.get(RESERVEDMAPKEY_PARAMETERS);
            Object execPolicy = map instanceof Map ? ((Map) map).get(RESERVEDMAPKEY_EXECPOLICY) : null;

            if (streamProvider != null && EXEC_POLICY_EXCLUSIVE.equals(execPolicy) &&
                    (KETTLE_JOB_ACTIONID.equals(actionId) || KETTLE_TRANS_ACTIONID.equals(actionId))) {
                return new StringBuilder()
                        .append(jobKey.getJobName()).append('.').append(jobKey.getUserName()).toString();
            }
        }

        return null;
    }
    */

    private static boolean compareParameters(Object map1, Object map2) {
        if (!(map1 instanceof Map) || !(map2 instanceof Map)) {
            return false;
        }

        Map m1 = (Map) map1;
        Map m2 = (Map) map2;

        boolean isSame = m1.size() == m2.size();

        if (isSame) {
            for (Object key : m1.keySet()) {
                if (IGNORABLE_KEYS.contains(key)) {
                    continue;
                }

                if (!(isSame = Objects.equals(m1.get(key), m2.get(key)))) {
                    break;
                }
            }
        }

        return isSame;
    }

    void applyRule(Scheduler scheduler, JobDetail jobDetail) throws JobExecutionException {
        QuartzJobKey jobKey = extractJobKey(jobDetail);

        if (scheduler == null || jobKey == null) {
            return;
        }

        JobDataMap params = jobDetail.getJobDataMap();
        String actionId = params.getString(QuartzScheduler.RESERVEDMAPKEY_ACTIONID);
        Object streamProvider = params.get(QuartzScheduler.RESERVEDMAPKEY_STREAMPROVIDER);
        streamProvider = streamProvider == null ? null : streamProvider.toString();
        Object map = params.get(RESERVEDMAPKEY_PARAMETERS);
        String execPolicy = map instanceof Map
                ? String.valueOf(((Map) map).get(RESERVEDMAPKEY_EXECPOLICY)) : EXEC_POLICY_DEFAULT;

        if (streamProvider != null && execPolicy.startsWith(EXEC_POLICY_EXCLUSIVE) &&
                (KETTLE_JOB_ACTIONID.equals(actionId) || KETTLE_TRANS_ACTIONID.equals(actionId))) {
            // trying to understand what's the action to take(block current execution or kill running jobs)
            for (ExclusiveKettleJobAction action : ExclusiveKettleJobAction.extractActions(jobKey, execPolicy)) {
                action.execute();
            }
            
            // now proceed general exclusive detection
            List<JobExecutionContext> executingJobs;
            try {
                executingJobs = (List<JobExecutionContext>) scheduler.getCurrentlyExecutingJobs();
            } catch (SchedulerException e) {
                executingJobs = new ArrayList<>(0);
            }

            for (JobExecutionContext ctx : executingJobs) {
                JobDetail detail = ctx.getJobDetail();
                if (jobDetail == detail) { // ignore the exact same job
                    continue;
                }

                QuartzJobKey key = extractJobKey(detail);
                JobDataMap dataMap = detail.getJobDataMap();

                if (logger.isDebugEnabled()) {
                    logger.debug(String.valueOf(ctx) + "\r\nTrigger = ["
                            + ctx.getTrigger() + "]\r\nSame Jobs ? " + (jobDetail == detail));
                }

                if (key != null &&
                        actionId.equals(dataMap.getString(QuartzScheduler.RESERVEDMAPKEY_ACTIONID)) &&
                        // FIXME this is tricky but might be better than comparing stream objects
                        // see https://github.com/pentaho/pentaho-platform/blob/6.1.0.1-R/extensions/src/org/pentaho/platform/web/http/api/resources/RepositoryFileStreamProvider.java
                        streamProvider.equals(String.valueOf(
                                dataMap.get(QuartzScheduler.RESERVEDMAPKEY_STREAMPROVIDER))) &&
                        jobKey.getJobName().equals(key.getJobName()) &&
                        jobKey.getUserName().equals(key.getUserName()) &&
                        jobDetail.getGroup().equals(detail.getGroup()) &&
                        compareParameters(map, dataMap.get(RESERVEDMAPKEY_PARAMETERS))) {

                    throw new JobExecutionException(new StringBuilder()
                            .append("Discard exclusive job [")
                            .append(jobKey)
                            .append("] because [")
                            .append(detail)
                            .append("] is running")
                            .toString());
                }
            }
        }
    }

    /*
    @Override
    public String getName() {
        return TRIGGER_LISTENER_NAME;
    }

    @Override
    public void triggerFired(Trigger trigger, JobExecutionContext jobExecutionContext) {
    }

    @Override
    public boolean vetoJobExecution(Trigger trigger, JobExecutionContext jobExecutionContext) {
        boolean vetoed = false;

        String jobName = getExclusiveJobName(jobExecutionContext);
        if (jobName != null) {
            Date lastFireTime = jobFireDates.get(jobName);
            Date fireTime = jobExecutionContext.getFireTime();
            if (lastFireTime != null && lastFireTime.compareTo(fireTime) < 0) {
                jobFireDates.put(jobName, fireTime);
                vetoed = true;
                if (logger.isWarnEnabled()) {
                    logger.warn(new StringBuilder()
                            .append("*** Cancel trigger fired at ")
                            .append(fireTime)
                            .append(" as exclusive job[")
                            .append(jobName).append("] is running since ")
                            .append(lastFireTime)
                            .append(" ***").toString());
                }
            }
        }

        return vetoed;
    }

    @Override
    public void triggerMisfired(Trigger trigger) {
    }

    @Override
    public void triggerComplete(Trigger trigger, JobExecutionContext jobExecutionContext, int i) {
        String jobName = getExclusiveJobName(jobExecutionContext);
        if (jobName != null) {
            Date fireTime = jobFireDates.remove(jobName);
            if (logger.isInfoEnabled()) {
                logger.info(new StringBuilder()
                        .append("===> Trigger fired at ")
                        .append(fireTime)
                        .append(" for exclusive job[")
                        .append(jobName).append("] is completed (instruction code = ")
                        .append(i)
                        .append(" <===").toString());
            }
        }
    }
    */
}
