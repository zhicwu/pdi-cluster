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
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionException;
import org.quartz.Scheduler;

import java.util.Map;

import static org.pentaho.platform.scheduler2.quartz.QuartzSchedulerHelper.*;

/**
 * Define a rule for injecting scheduled job id into the underlying job.
 *
 * @author Zhichun Wu
 */
public class JobIdInjectionRule {
    static final JobIdInjectionRule instance = new JobIdInjectionRule();

    private static final Log logger = LogFactory.getLog(JobIdInjectionRule.class);

    void applyRule(Phase phase, Scheduler scheduler, JobDetail jobDetail) throws JobExecutionException {
        QuartzJobKey jobKey = extractJobKey(jobDetail);

        if (scheduler == null || jobKey == null) {
            return;
        }

        JobDataMap params = jobDetail.getJobDataMap();

        String actionId = params.getString(QuartzScheduler.RESERVEDMAPKEY_ACTIONID);
        String lineAgeId = params.getString(QuartzScheduler.RESERVEDMAPKEY_LINEAGE_ID);
        Object map = params.get(RESERVEDMAPKEY_PARAMETERS);
        Map jobParams = map instanceof Map ? (Map) map : null;
        String etlScript = extractString(jobParams, KEY_ETL_SCRIPT);

        if ((KETTLE_JOB_ACTIONID.equals(actionId) || KETTLE_TRANS_ACTIONID.equals(actionId))
                && etlScript != null) {
            jobParams.put(KEY_ETL_JOB_ID, jobKey.toString());
            jobParams.put(KEY_ETL_TRACE_ID, lineAgeId);
        }
    }
}
