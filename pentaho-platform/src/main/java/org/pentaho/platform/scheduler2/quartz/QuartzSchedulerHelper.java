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

import org.quartz.JobDetail;
import org.quartz.JobExecutionException;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Utility class for QuartzShceduler.
 *
 * @author Zhichun Wu
 */
public final class QuartzSchedulerHelper {
    static final String RESERVEDMAPKEY_PARAMETERS = "parameters";
    static final String RESERVEDMAPKEY_EXECPOLICY = "executionPolicy";

    static final String EXEC_POLICY_DEFAULT = "Unrestricted";
    // typical usage of exclusive rule:
    // 1) Exclusive(respect=Job1,Job2,Job3...)
    // 2) Exclusive(kill=Job1,Job2,Job3...)
    // 3) Exclusive(kill=Job1;respect=Job2,Job3...)
    // 4) Exclusive(Kill=Job1;Kill=Job2;Respect=Job3;Kill=Job4...)
    static final String EXEC_POLICY_EXCLUSIVE = "Exclusive";

    static final String KETTLE_JOB_ACTIONID = "kjb.backgroundExecution";
    static final String KETTLE_TRANS_ACTIONID = "ktr.backgroundExecution";

    static final String KEY_ETL_SCRIPT = System.getProperty("KETTLE_JOB_NAME_KEY", "ETL_SCRIPT");
    static final String KEY_ETL_JOB_ID = System.getProperty("KETTLE_JOB_ID_KEY", "ETL_CALLER");
    static final String KEY_ETL_TRACE_ID = System.getProperty("KETTLE_TRACE_ID_KEY", "UNIQUE_ID");

    static final int KETTLE_JOB_KILLER_MAX_WAIT
            = Integer.parseInt(System.getProperty("KETTLE_JOB_KILLER_WAIT_SEC", "8000"));
    static final int KETTLE_JOB_KILLER_CHECK_INTERVAL
            = Integer.parseInt(System.getProperty("KETTLE_JOB_KILLER_CHECK_INTERVAL", "2000"));

    static final Set<String> IGNORABLE_KEYS = new HashSet<>(Arrays.asList(KEY_ETL_JOB_ID, KEY_ETL_TRACE_ID));

    static String extractString(Map map, String key) {
        return extractString(map, key, null);
    }

    static String extractString(Map map, String key, String defaultValue) {
        String value = defaultValue;
        if (map != null) {
            Object obj = map.get(key);
            if (obj != null) {
                value = String.valueOf(obj);
            }
        }

        return value;
    }

    static QuartzJobKey extractJobKey(JobDetail jobDetail) {
        QuartzJobKey jobKey = null;
        try {
            jobKey = jobDetail == null ? null : QuartzJobKey.parse(jobDetail.getName());
        } catch (org.pentaho.platform.api.scheduler2.SchedulerException e) {
            // ignore error
        }

        return jobKey;
    }

    // http://stackoverflow.com/questions/19733981/quartz-skipping-duplicate-job-fires-scheduled-with-same-fire-time
    static void init(Scheduler scheduler) throws SchedulerException {
        if (scheduler == null) {
            return;
        }

        // attached listeners even the scheduler is shutdown
        // scheduler.addTriggerListener(ExclusiveKettleJobRule.instance);
    }

    // http://stackoverflow.com/questions/2676295/quartz-preventing-concurrent-instances-of-a-job-in-jobs-xml
    static void applyJobExecutionRules(Scheduler scheduler, JobDetail jobDetail) throws JobExecutionException {
        JobIdInjectionRule.instance.applyRule(scheduler, jobDetail);
        ExclusiveKettleJobRule.instance.applyRule(scheduler, jobDetail);
    }
}
