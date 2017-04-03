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
import org.pentaho.di.core.parameters.NamedParams;
import org.pentaho.di.core.parameters.UnknownParamException;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.www.GetCacheStatusServlet;
import org.pentaho.di.www.SlaveServerJobStatus;
import org.pentaho.di.www.SlaveServerTransStatus;
import org.pentaho.di.www.WebResult;

import javax.servlet.http.HttpServletRequest;
import java.net.URLEncoder;
import java.util.AbstractMap;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Cache for three key types of resource: transformation, job and data source.
 *
 * @author Zhichun Wu
 */
public final class ServerCache {
    public static final boolean RESOURCE_CACHE_DISABLED = "Y".equalsIgnoreCase(
            System.getProperty("KETTLE_RESOURCE_CACHE_DISABLED", "N"));
    public static final int RESOURCE_CACHE_SIZE
            = Integer.parseInt(System.getProperty("KETTLE_RESOURCE_CACHE_SIZE", "100"));
    public static final int RESOURCE_EXPIRATION_MINUTE
            = Integer.parseInt(System.getProperty("KETTLE_RESOURCE_EXPIRATION_MINUTE", "1800"));
    public static final String PARAM_ETL_JOB_ID = System.getProperty("KETTLE_JOB_ID_KEY", "ETL_CALLER");

    static final String KEY_ETL_CACHE_ID = System.getProperty("KETTLE_CACHE_ID_KEY", "CACHE_ID");
    static final String KEY_ETL_REQUEST_ID = System.getProperty("KETTLE_REQUEST_ID_KEY", "REQUEST_ID");

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

    private static String buildResourceName(AbstractMeta meta, Map<String, String> params, SlaveServer server) {
        StringBuilder sb = new StringBuilder();

        if (RESOURCE_CACHE_DISABLED) {
            return sb.toString();
        }

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

        Date modifiedDate = meta.getModifiedDate();
        Date creationDate = meta.getCreatedDate();
        sb.append('-').append(
                (modifiedDate != null && creationDate != null && modifiedDate.after(creationDate))
                        ? modifiedDate.getTime()
                        : (creationDate == null ? -1 : creationDate.getTime()));

        String host = server == null ? null : server.getHostname();
        String port = server == null ? null : server.getPort();
        VariableSpace space = server.getParentVariableSpace();
        if (space != null) {
            host = space.environmentSubstitute(host);
            port = space.environmentSubstitute(port);
        }

        return sb.append('@').append(host).append(':').append(port).toString();
    }

    public static Map<String, String> buildRequestParameters(String resourceName,
                                                             Map<String, String> params,
                                                             Map<String, String> vars) {
        Map<String, String> map = new HashMap<String, String>();

        if (!Strings.isNullOrEmpty(resourceName)) {
            map.put(KEY_ETL_CACHE_ID, resourceName);
        }

        if (params != null) {
            String requestId = params.get(KEY_ETL_REQUEST_ID);
            if (!Strings.isNullOrEmpty(requestId)) {
                map.put(KEY_ETL_REQUEST_ID, requestId);
            }
        }

        if (vars != null) {
            String requestId = vars.get(KEY_ETL_REQUEST_ID);
            if (!Strings.isNullOrEmpty(requestId)) {
                map.put(KEY_ETL_REQUEST_ID, requestId);
            }
        }

        return map;
    }

    public static void updateParametersAndCache(HttpServletRequest request,
                                                NamedParams params,
                                                VariableSpace vars,
                                                String carteObjectId) {
        String cacheId = request == null ? null : request.getHeader(KEY_ETL_CACHE_ID);
        String requestId = request == null ? null : request.getHeader(KEY_ETL_REQUEST_ID);

        if (!Strings.isNullOrEmpty(requestId)) {
            try {
                params.setParameterValue(KEY_ETL_REQUEST_ID, requestId);
            } catch (UnknownParamException e) {
                // this should not happen
            }

            if (vars != null) {
                vars.setVariable(KEY_ETL_REQUEST_ID, requestId);
            }
        }

        // update cache
        if (!Strings.isNullOrEmpty(cacheId) && !Strings.isNullOrEmpty(carteObjectId)) {
            cacheIdentity(cacheId, carteObjectId);
        }
    }

    /**
     * Retrieve a unique id generated for the given resource if it's been cached.
     *
     * @param resourceName name of the resource, usually a file name(for job and trans)
     * @return
     */
    public static String getCachedIdentity(String resourceName) {
        return RESOURCE_CACHE_DISABLED ? null : resourceCache.getIfPresent(resourceName);
    }

    public static Map.Entry<String, String> getCachedEntry(
            AbstractMeta meta, Map<String, String> params, SlaveServer server) {
        String resourceName = buildResourceName(meta, params, server);
        String identity = getCachedIdentity(resourceName);

        if (Strings.isNullOrEmpty(identity)) {
            // don't give up so quick as this might be cached on slave server
            try {
                String reply =
                        server.execService(GetCacheStatusServlet.CONTEXT_PATH + "/?name="
                                + URLEncoder.encode(resourceName, "UTF-8"));
                WebResult webResult = WebResult.fromXMLString(reply);
                if (webResult.getResult().equalsIgnoreCase(WebResult.STRING_OK)) {
                    identity = webResult.getId();
                    logBasic(server,
                            new StringBuilder().append("Found ").append(resourceName).append('=').append(identity)
                                    .append(" on remote slave server").toString());
                }
            } catch (Exception e) {
                // ignore as this is usually a network issue
            }
        }

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

        return new AbstractMap.SimpleImmutableEntry<String, String>(resourceName, identity);
    }

    /**
     * Cache the identity.
     *
     * @param resourceName resource name
     * @param identity     identity
     */
    public static void cacheIdentity(String resourceName, String identity) {
        if (!RESOURCE_CACHE_DISABLED) {
            resourceCache.put(resourceName, identity);
        }
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
