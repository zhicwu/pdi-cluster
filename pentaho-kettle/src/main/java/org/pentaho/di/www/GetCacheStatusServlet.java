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
import org.pentaho.di.core.Const;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Get cache status mainly for three different types of resource: transformation, job and data source.
 *
 * @author Zhichun Wu
 */
public class GetCacheStatusServlet extends BaseHttpServlet implements CartePluginInterface {
    private static final Class<?> PKG = GetCacheStatusServlet.class; // for i18n purposes, needed by Translator2!!

    private static final String XML_CONTENT_TYPE = "text/xml";

    private static final long serialVersionUID = -519824343678414598L;

    public static final String CONTEXT_PATH = "/kettle/cache";
    public static final String PARAM_NAME = "name";
    public static final String PARAM_INVALIDATE = "invalidate";

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

        String resourceName = request.getParameter(PARAM_NAME);
        boolean applyToAll = Strings.isNullOrEmpty(resourceName);
        boolean invalidate = "Y".equalsIgnoreCase(request.getParameter(PARAM_INVALIDATE));

        WebResult result = new WebResult(WebResult.STRING_OK, "", "");

        if (invalidate) {
            if (applyToAll) {
                ServerCache.invalidateAll();
            } else {
                ServerCache.invalidate(resourceName);
            }
        } else { // just about queries
            if (applyToAll) {
                // keep the id empty, add information into description
                result.setMessage(ServerCache.getStats());
            } else {
                String identity = ServerCache.getCachedIdentity(resourceName);
                if (!Strings.isNullOrEmpty(identity)) {
                    result.setId(identity);
                }
            }
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
