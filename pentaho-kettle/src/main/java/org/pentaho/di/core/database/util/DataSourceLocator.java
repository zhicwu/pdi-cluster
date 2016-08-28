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
package org.pentaho.di.core.database.util;

import org.osjava.sj.loader.SJDataSource;
import org.pentaho.database.IDatabaseDialect;
import org.pentaho.database.dialect.GenericDatabaseDialect;
import org.pentaho.database.model.DatabaseAccessType;
import org.pentaho.database.model.IDatabaseConnection;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.www.CarteSingleton;

import javax.naming.Context;
import javax.sql.DataSource;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Utility class for data source lookup.
 *
 * @author Zhichun Wu
 */
public final class DataSourceLocator {
    private static boolean activated = false;

    private static final AtomicBoolean changed = new AtomicBoolean(false);
    private static final Map<String, DataSource> dataSources
            = Collections.synchronizedMap(new HashMap<String, DataSource>());

    private static final ServiceLoader<IDatabaseDialect> dialectLoader = ServiceLoader.load(IDatabaseDialect.class);

    private static DataSource buildDataSource(IDatabaseConnection conn) throws Exception {
        String dbType = conn.getDatabaseType().getShortName();

        SJDataSource ds = null;

        for (IDatabaseDialect dialect : dialectLoader) {
            // FIXME fallback to name/desc like Kettle did?
            if (!dbType.equals(dialect.getDatabaseType().getShortName())) {
                continue;
            }

            ds = new SJDataSource(dialect instanceof GenericDatabaseDialect
                    ? conn.getAttributes().get(GenericDatabaseDialect.ATTRIBUTE_CUSTOM_DRIVER_CLASS)
                    : dialect.getNativeDriver(),
                    dialect.supportsOptionsInURL() ? dialect.getURLWithExtraOptions(conn) : dialect.getURL(conn),
                    conn.getUsername(),
                    conn.getPassword(),
                    new Properties());
            break;
        }

        return ds;
    }

    /**
     * This method tries to import data sources defined in Kettle master time after time.
     * It does nothing in non-cluster environment, but it addresses data source configuration issue in Kettle cluster:
     * - zero data source configuration in Kettle slave servers - everything comes from master
     * - cache data source configuration in memory for a while for better performance
     * - update cached data source in case Kettle master changed configuration of a certain data source
     *
     * @param dsCache shared in-memory cache of data sources regardless where it comes from
     * @param ctx     naming context for binding / rebinding data source from master
     * @param dsName  name of the data source
     */
    static void importDataSourcesFromMaster(Map<String, DataSource> dsCache, Context ctx, String dsName) {
        if (!activated) {
            return;
        }

        // this is not truly thread-safe, as data sources might be updated in the same time
        // however, it is worthy of doing this for better performance
        if (changed.compareAndSet(true, false)) {
            dsCache.putAll(dataSources);

            LogChannelInterface log = CarteSingleton.getInstance().getLog();
            log.logBasic("Data source cache refreshed successfully");
        }
    }

    public static void activate() {
        activated = true;
    }

    public static void deactivate() {
        activated = false;
    }


    public static int updateDataSourceMappings(Map<String, IDatabaseConnection> mapping) {
        if (mapping == null || mapping.size() == 0) {
            return 0;
        }

        LogChannelInterface log = CarteSingleton.getInstance().getLog();

        int counter = 0;
        for (Map.Entry<String, IDatabaseConnection> entry : mapping.entrySet()) {
            String dsName = entry.getKey();
            IDatabaseConnection ds = entry.getValue();

            if (ds.getAccessType() == DatabaseAccessType.NATIVE && ds.getDatabaseType() != null) {
                try {
                    DataSource d = buildDataSource(ds);
                    if (d == null) {
                        log.logError("Bypass unsupported data source: " + dsName);
                        continue;
                    }

                    // it's better to use a separated map here but we should be just fine
                    dataSources.put(dsName, d);
                    counter++;
                } catch (Exception e) {
                    log.logError("Failed to build data source: " + dsName, e);
                }
            }
        }

        if (counter > 0) {
            changed.set(true);
        }

        return counter;
    }

    private DataSourceLocator() {
    }
}
