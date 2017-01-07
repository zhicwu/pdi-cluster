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
package org.pentaho.di.job.entries;

import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.logging.LogChannelInterface;

/**
 * Utility class for streaming log entries from slave back to master.
 *
 * @author Zhichun Wu
 */
public final class RemoteJobEntryLogHelper {
    private static final String UNKNOWN_SERVER = "unknown server";
    private static final String UNKNOWN_OBJECT = "unknown object";

    private final LogChannelInterface logger;

    private final String serverAddress;
    private final String objectId;

    private int lastLogEntryNo;

    public RemoteJobEntryLogHelper(SlaveServer server, String objectId, LogChannelInterface logger) {
        this.logger = logger;

        this.serverAddress = server == null || server.getName() == null ? UNKNOWN_SERVER : server.getName();
        this.objectId = objectId == null ? UNKNOWN_OBJECT : objectId;

        this.lastLogEntryNo = 0;

        if (this.logger != null) {
            this.logger.logBasic(new StringBuilder()
                    .append("=== Log Replayer is ready for [")
                    .append(this.objectId)
                    .append('@')
                    .append(serverAddress)
                    .append("] ===").toString());
        }
    }

    public int getLastLogEntryNo() {
        return this.lastLogEntryNo;
    }

    public void log(String logString, int firstEntryLineNo, int lastEntryLineNo) {
        if (logger == null || logString == null) {
            return;
        }

        int length = logString.length();

        if (lastLogEntryNo != lastEntryLineNo) {
            if (length > 0) {
                logger.logBasic(new StringBuilder()
                        .append("---> Replay logs from #")
                        .append(firstEntryLineNo)
                        .append(" to #")
                        .append(lastEntryLineNo)
                        .append(": ")
                        .append(length)
                        .append(" bytes <---").toString());

                logger.logBasic(logString);
            }

            int lineDiff = firstEntryLineNo - lastLogEntryNo;

            if (lineDiff != 0) {
                logger.logError(new StringBuffer()
                        .append("*** Skip ")
                        .append(lineDiff) // could be negative
                        .append(" lines from #")
                        .append(firstEntryLineNo)
                        .append(" to #")
                        .append(lastLogEntryNo)
                        .append(" ***")
                        .toString());
            }

            lastLogEntryNo = lastEntryLineNo;
        }
    }
}
