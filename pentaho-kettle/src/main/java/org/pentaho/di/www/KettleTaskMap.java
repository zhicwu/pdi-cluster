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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.logging.HasLogChannelInterface;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.logging.LoggingRegistry;
import org.pentaho.di.core.util.EnvUtil;
import org.pentaho.di.core.util.Utils;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Abstracted class for building JobMap and TransformationMap.
 *
 * @author Zhichun Wu
 */
public class KettleTaskMap<E extends HasLogChannelInterface, C> {
    private static final int KETTLE_JOB_TRANS_LIST_SIZE
            = Integer.parseInt(System.getProperty("KETTLE_JOB_TRANS_LIST_SIZE", "200"));

    private static final RemovalListener<CarteObjectEntry, EntryInfo> defaultRemovalListener
            = new RemovalListener<CarteObjectEntry, EntryInfo>() {
        public void onRemoval(RemovalNotification<CarteObjectEntry, EntryInfo> removal) {
            removal.getValue().dispose(removal.getKey());
        }
    };

    static class EntryInfo<E extends HasLogChannelInterface, C> {
        E entry;
        C config;

        EntryInfo(E entry, C config) {
            this.entry = entry;
            this.config = config;
        }

        void dispose(CarteObjectEntry id) {
            if (this.entry != null) {
                String logChannelId = null;
                try {
                    LogChannelInterface logger = this.entry.getLogChannel();
                    logChannelId = logger.getLogChannelId();

                    logger.logBasic(
                            new StringBuilder("Removing entry[").append(this.entry)
                                    .append("] (CarteObjectId=").append(id.getId())
                                    .append(")...").toString());
                } catch (Exception e) {
                    // ignore error here
                }

                if (logChannelId != null) {
                    // Remove the logging information from the log registry & central log store
                    //
                    KettleLogStore.discardLines(logChannelId, false);
                    LoggingRegistry.getInstance().removeIncludingChildren(logChannelId);
                }
            }

            this.entry = null;
            this.config = null;
        }
    }

    final Cache<CarteObjectEntry, EntryInfo> cache;

    KettleTaskMap(SlaveServerConfig config) {
        final int objectTimeout;
        String systemTimeout = EnvUtil.getSystemProperty(Const.KETTLE_CARTE_OBJECT_TIMEOUT_MINUTES, null);

        // The value specified in XML takes precedence over the environment variable!
        //
        if (config != null && config.getObjectTimeoutMinutes() > 0) {
            objectTimeout = config.getObjectTimeoutMinutes();
        } else if (!Utils.isEmpty(systemTimeout)) {
            objectTimeout = Const.toInt(systemTimeout, 1440);
        } else {
            objectTimeout = 24 * 60; // 1440 : default is a one day time-out
        }

        this.cache = CacheBuilder.newBuilder()
                .concurrencyLevel(1)
                .maximumSize(KETTLE_JOB_TRANS_LIST_SIZE)
                .expireAfterWrite(objectTimeout, TimeUnit.MINUTES)
                .removalListener(defaultRemovalListener)
                .recordStats()
                .build();
    }

    EntryInfo<E, C> createEntry(E entry, C config) {
        return new EntryInfo<>(entry, config);
    }

    String getStats() {
        StringBuilder sb = new StringBuilder(cache.stats().toString());

        try {
            Map<CarteObjectEntry, EntryInfo> map = cache.asMap();
            for (CarteObjectEntry key : map.keySet()) {
                sb.append(Const.CR).append(key);
            }
        } catch (Exception e) {
            // ignore
        }

        return sb.toString();
    }

    void clear() {
        cache.invalidateAll();
    }
}
