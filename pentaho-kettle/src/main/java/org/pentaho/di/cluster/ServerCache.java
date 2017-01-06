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

package org.pentaho.di.cluster;

import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.pentaho.di.base.AbstractMeta;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.repository.ObjectRevision;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.www.SlaveServerJobStatus;
import org.pentaho.di.www.SlaveServerTransStatus;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Cache for three key types of resource: transformation, job and data source.
 *
 * @author Zhichun Wu
 */
public final class ServerCache {
    public static final int RESOURCE_CACHE_SIZE
            = Integer.parseInt(System.getProperty("RESOURCE_CACHE_SIZE", "500"));
    public static final int RESOURCE_EXPIRATION_MINUTE
            = Integer.parseInt(System.getProperty("RESOURCE_EXPIRATION_MINUTE", "780"));
    public static final String PARAM_ETL_JOB_ID = System.getProperty("KETTLE_JOB_ID_KEY", "ETL_CALLER");

    // On master node, it's for name -> revision + md5; on slave server, it's name -> md5
    private static final Cache<String, String> resourceCache = CacheBuilder.newBuilder()
            .maximumSize(RESOURCE_CACHE_SIZE)
            .expireAfterAccess(RESOURCE_EXPIRATION_MINUTE, TimeUnit.MINUTES)
            .recordStats()
            .build();

    private static final String UNKNOWN_RESOURCE = "n/a";

    private static void logBasic(SlaveServer server, String message) {
        LogChannelInterface logger = server == null ? null : server.getLogChannel();
        if (logger != null) {
            logger.logBasic(message);
        }
    }

    public static String buildResourceName(AbstractMeta meta, Map<String, String> params, SlaveServer server) {
        StringBuilder sb = new StringBuilder();

        // in case this is triggered by a Quartz Job
        String jobId = params == null ? null : params.get(PARAM_ETL_JOB_ID);
        if (Strings.isNullOrEmpty(jobId)) {
            if (meta != null) {
                sb.append(meta.getClass().getSimpleName()).append('-').append(meta.getName());
            } else {
                sb.append(UNKNOWN_RESOURCE);
            }
        } else {
            sb.append(jobId.replace('\t', '-'));
        }

        ObjectRevision revision = meta == null ? null : meta.getObjectRevision();
        Date creationDate = revision == null ? null : revision.getCreationDate();
        sb.append('-').append(creationDate == null ? -1 : creationDate.getTime());

        String host = server == null ? null : server.getHostname();
        String port = server == null ? null : server.getPort();
        VariableSpace space = server.getParentVariableSpace();
        if (space != null) {
            host = space.environmentSubstitute(host);
            port = space.environmentSubstitute(port);
        }

        return sb.append('@').append(host).append(':').append(port).toString();
    }

    /**
     * Retrieve a unique id generated for the given resource if it's been cached.
     *
     * @param resourceName name of the resource, usually a file name(for job and trans)
     * @return
     */
    public static String getCachedIdentity(String resourceName) {
        return resourceCache.getIfPresent(resourceName);
    }

    public static String getCachedIdentity(AbstractMeta meta, Map<String, String> params, SlaveServer server) {
        String resourceName = buildResourceName(meta, params, server);
        String identity = getCachedIdentity(resourceName);

        // let's see if the slave server still got this
        if (!Strings.isNullOrEmpty(identity)) {
            try {
                if (meta instanceof JobMeta) {
                    SlaveServerJobStatus status = server.getJobStatus(meta.getName(), identity, Integer.MAX_VALUE);
                    if (status.getResult() == null) { // it's possible that the job is still running
                        logBasic(server,
                                new StringBuilder()
                                        .append(resourceName).append('=').append(identity)
                                        .append(" is invalidated due to status [")
                                        .append(status.getStatusDescription()).append(']').toString());
                        invalidate(resourceName);
                        identity = null;
                    }
                } else if (meta instanceof TransMeta) {
                    SlaveServerTransStatus status = server.getTransStatus(meta.getName(), identity, Integer.MAX_VALUE);
                    if (status.getResult() == null) { // it's possible that the trans is still running
                        logBasic(server,
                                new StringBuilder()
                                        .append(resourceName).append('=').append(identity)
                                        .append(" is invalidated due to status [")
                                        .append(status.getStatusDescription()).append(']').toString());
                        invalidate(resourceName);
                        identity = null;
                    }
                } // who knows if someday there's a new type...
            } catch (Exception e) {
                // ignore as this is usually a network issue
            }
        }

        return identity;
    }

    /**
     * Cache the identity.
     *
     * @param resourceName resource name
     * @param identity     identity
     */
    public static void cacheIdentity(String resourceName, String identity) {
        resourceCache.put(resourceName, identity);
    }

    public static void cacheIdentity(AbstractMeta meta, Map<String, String> params, SlaveServer server, String identity) {
        cacheIdentity(buildResourceName(meta, params, server), identity);
    }

    public static void invalidate(AbstractMeta meta, Map<String, String> params, SlaveServer server) {
        invalidate(buildResourceName(meta, params, server));
    }

    public static void invalidate(String resourceName) {
        resourceCache.invalidate(resourceName);
    }

    public static void invalidateAll() {
        resourceCache.invalidateAll();
    }

    public static String getStats() {
        StringBuilder sb = new StringBuilder(resourceCache.stats().toString());

        try {
            Map<String, String> map = resourceCache.asMap();
            for (String key : map.keySet()) {
                sb.append(Const.CR).append(key);
            }
        } catch (Exception e) {
            // ignore
        }

        return sb.toString();
    }

    private ServerCache() {
    }
}
