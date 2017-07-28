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

import com.google.common.base.Strings;
import org.pentaho.di.cluster.ServerCache;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.Const;
import org.pentaho.di.resource.ResourceDefinitionHelper;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ConcurrentModificationException;
import java.util.List;

/**
 * Get cache status mainly for four different types of resource: class, transformation, job, metadata and serialized package.
 *
 * @author Zhichun Wu
 */
public class GetCacheStatusServlet extends BaseHttpServlet implements CartePluginInterface {
    private static final Class<?> PKG = GetCacheStatusServlet.class; // for i18n purposes, needed by Translator2!!

    private static final String XML_CONTENT_TYPE = "text/xml";

    private static final String UDJC_CLASS_NAME = "org.pentaho.di.trans.steps.userdefinedjavaclass.UserDefinedJavaClassMeta";
    private static final String UDJC_READ_METHOD_NAME = "getCacheStats";
    private static final String UDJC_WRITE_METHOD_NAME = "invalidateCache";
    private static final String DEFAULT_CACHE_STATS = "N/A";
    private static final String CLASS_CACHE_NAME = "Class Cache: ";
    private static final String PACKAGE_CACHE_NAME = "Package Cache: ";

    private static final long serialVersionUID = -519824343678414598L;

    public static final String CONTEXT_PATH = "/kettle/cache";
    public static final String PARAM_TYPE = "type";
    public static final String PARAM_NAME = "name";
    public static final String PARAM_INVALIDATE = "invalidate";

    public static final String CACHE_TYPE_CLASS = "class";
    public static final String CACHE_TYPE_META = "meta";
    public static final String CACHE_TYPE_PACKAGE = "package";

    private void invalidateCache(String cacheType, String entryName) {
        boolean defaultType = Strings.isNullOrEmpty(cacheType);

        if (CACHE_TYPE_CLASS.equalsIgnoreCase(cacheType)) {
            try {
                Class clazz = Class.forName(UDJC_CLASS_NAME);
                Method method = clazz.getMethod(UDJC_WRITE_METHOD_NAME);
                method.invoke(null);
            } catch (Exception e) {
                // ignore
            }
        } else if (defaultType || CACHE_TYPE_PACKAGE.equalsIgnoreCase(cacheType)) {
            if (!Strings.isNullOrEmpty(entryName)) {
                ServerCache.invalidateInLocal(entryName);
            } else {
                ServerCache.invalidateAllInLocal();
            }
        }

        // propagate to slave servers if and only if this is master node
        SlaveServer currentServer = CarteSingleton.getSlaveServerConfig().getSlaveServer();

        if (currentServer.isMaster() || currentServer.getHostname() == null) {
            List<SlaveServerDetection> detections = getDetections();
            if (detections != null) {
                StringBuilder desc = new StringBuilder(CONTEXT_PATH)
                        .append('?').append(PARAM_INVALIDATE).append('=').append('Y');
                if (cacheType != null) {
                    desc.append('&').append(PARAM_TYPE).append('=').append(cacheType);
                }
                if (entryName != null) {
                    desc.append('&').append(PARAM_NAME).append('=').append(entryName);
                }
                String serviceDesc = desc.toString();

                try {
                    for (SlaveServerDetection detectedServer : detections) {
                        SlaveServer server = detectedServer.getSlaveServer();
                        try {
                            server.execService(serviceDesc);
                        } catch (Exception e) {
                            // ignore
                        }
                    }
                } catch (ConcurrentModificationException e) {
                    // this is fine as we trade thread-safty for performance
                }
            }
        }
    }

    private void fillCacheStats(String cacheType, String entryName, WebResult result) {
        StringBuilder sb = new StringBuilder();
        boolean all = Strings.isNullOrEmpty(cacheType);

        if (all || CACHE_TYPE_CLASS.equalsIgnoreCase(cacheType)) {
            sb.append(CLASS_CACHE_NAME);
            try {
                Class clazz = Class.forName(UDJC_CLASS_NAME);
                Method method = clazz.getMethod(UDJC_READ_METHOD_NAME);
                sb.append(method.invoke(null));
            } catch (Exception e) {
                sb.append(DEFAULT_CACHE_STATS);
            }
            sb.append('\r').append('\n');
        }

        if (all || CACHE_TYPE_PACKAGE.equalsIgnoreCase(cacheType)) {
            if (!Strings.isNullOrEmpty(entryName)) {
                String identity = ServerCache.getCachedIdentity(entryName);
                if (!Strings.isNullOrEmpty(identity)) {
                    result.setId(identity);
                } else {
                    result.setResult(WebResult.STRING_ERROR);
                    result.setMessage(
                            new StringBuilder().append('[').append(entryName).append("] not found").toString());
                    return;
                }
            } else {
                sb.append(PACKAGE_CACHE_NAME).append(ServerCache.getStats());
            }
        }

        result.setMessage(sb.toString());
    }

    public GetCacheStatusServlet() {
    }

    public GetCacheStatusServlet(TransformationMap transformationMap, JobMap jobMap) {
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

        String cacheType = request.getParameter(PARAM_TYPE);
        String entryName = request.getParameter(PARAM_NAME);

        WebResult result = new WebResult(WebResult.STRING_OK, "", "");

        if ("Y".equalsIgnoreCase(request.getParameter(PARAM_INVALIDATE))) {
            invalidateCache(cacheType, entryName);
        } else { // just about queries
            fillCacheStats(cacheType, entryName, result);
        }

        response.getWriter().print(result.getXML());
    }

    public String toString() {
        return "Cache Handler";
    }

    public String getService() {
        return CONTEXT_PATH + " (" + toString() + ")";
    }

    public String getContextPath() {
        return CONTEXT_PATH;
    }
}
