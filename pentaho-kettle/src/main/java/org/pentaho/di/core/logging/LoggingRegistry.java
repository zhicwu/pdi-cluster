/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
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

package org.pentaho.di.core.logging;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.util.EnvUtil;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class LoggingRegistry {
    protected final static List<String> EMPTY_LIST = new ArrayList<>(0);
    // OK, why not declare the followings as final?
    private static final LoggingRegistry registry = new LoggingRegistry();
    private static final int DEFAULT_MAX_SIZE = 10000;

    private final Map<String, LoggingObjectInterface> map;
    //private Map<String, LogChannelFileWriterBuffer> fileWriterBuffers;
    private final Map<String, List<String>> childrenMap;
    private final int maxSize;
    private final Object syncObject = new Object();

    private Date lastModificationTime;

    private LoggingRegistry() {
        this.map = new ConcurrentHashMap<String, LoggingObjectInterface>();
        //this.fileWriterBuffers = new ConcurrentHashMap<>();
        this.childrenMap = new ConcurrentHashMap<String, List<String>>();

        this.lastModificationTime = new Date();
        this.maxSize = Const.toInt(EnvUtil.getSystemProperty("KETTLE_MAX_LOGGING_REGISTRY_SIZE"), DEFAULT_MAX_SIZE);
    }

    public static LoggingRegistry getInstance() {
        return registry;
    }

    public String registerLoggingSource(Object object) {
        synchronized (this.syncObject) {
            // it does not make sense to me to check the setting every time registering a logging service,
            // not to mention I once had the weird thread dump below(all threads were locked up):
            // Thread 28307: (state = BLOCKED)
            // - java.lang.Integer.parseInt(java.lang.String, int) @bci=4, line=542 (Compiled frame)
            // - java.lang.Integer.parseInt(java.lang.String) @bci=3, line=615 (Compiled frame)
            // - org.pentaho.di.core.Const.toInt(java.lang.String, int) @bci=1, line=1173 (Compiled frame)
            // - org.pentaho.di.core.logging.LoggingRegistry.registerLoggingSource(java.lang.Object) @bci=16, line=63 (Compiled frame)
            // ...

            // this.maxSize = Const.toInt( EnvUtil.getSystemProperty( "KETTLE_MAX_LOGGING_REGISTRY_SIZE" ), 10000 );

            LoggingObject loggingSource = new LoggingObject(object);

            LoggingObjectInterface found = findExistingLoggingSource(loggingSource);
            if (found != null) {
                LoggingObjectInterface foundParent = found.getParent();
                LoggingObjectInterface loggingSourceParent = loggingSource.getParent();
                if (foundParent != null && loggingSourceParent != null) {
                    String foundParentLogChannelId = foundParent.getLogChannelId();
                    String sourceParentLogChannelId = loggingSourceParent.getLogChannelId();
                    if (foundParentLogChannelId != null && sourceParentLogChannelId != null
                            && foundParentLogChannelId.equals(sourceParentLogChannelId)) {
                        String foundLogChannelId = found.getLogChannelId();
                        if (foundLogChannelId != null) {
                            return foundLogChannelId;
                        }
                    }
                }
            }

            String logChannelId = UUID.randomUUID().toString();
            loggingSource.setLogChannelId(logChannelId);

            this.map.put(logChannelId, loggingSource);

            if (loggingSource.getParent() != null) {
                String parentLogChannelId = loggingSource.getParent().getLogChannelId();
                if (parentLogChannelId != null) {
                    List<String> parentChildren = this.childrenMap.get(parentLogChannelId);
                    if (parentChildren == null) {
                        parentChildren = new ArrayList<String>();
                        this.childrenMap.put(parentLogChannelId, parentChildren);
                    }
                    parentChildren.add(logChannelId);
                }
            }

            this.lastModificationTime = new Date();
            loggingSource.setRegistrationDate(this.lastModificationTime);

            if ((this.maxSize > 0) && (this.map.size() > this.maxSize)) {
                List<LoggingObjectInterface> all = new ArrayList<LoggingObjectInterface>(this.map.values());
                Collections.sort(all, new Comparator<LoggingObjectInterface>() {
                    @Override
                    public int compare(LoggingObjectInterface o1, LoggingObjectInterface o2) {
                        if ((o1 == null) && (o2 != null)) {
                            return -1;
                        }
                        if ((o1 != null) && (o2 == null)) {
                            return 1;
                        }
                        if ((o1 == null) && (o2 == null)) {
                            return 0;
                        }
                        if (o1.getRegistrationDate() == null && o2.getRegistrationDate() != null) {
                            return -1;
                        }
                        if (o1.getRegistrationDate() != null && o2.getRegistrationDate() == null) {
                            return 1;
                        }
                        if (o1.getRegistrationDate() == null && o2.getRegistrationDate() == null) {
                            return 0;
                        }
                        return (o1.getRegistrationDate().compareTo(o2.getRegistrationDate()));
                    }
                });
                int cutCount = this.maxSize < 1000 ? this.maxSize : 1000;
                //List<String> channelsNotToRemove = getLogChannelFileWriterBufferIds();
                for (int i = 0; i < cutCount; i++) {
                    LoggingObjectInterface toRemove = all.get(i);
                    //if (!channelsNotToRemove.contains(toRemove.getLogChannelId())) {
                    this.map.remove(toRemove.getLogChannelId());
                    //}
                }
                removeOrphans();
            }
            return logChannelId;
        }
    }

    public LoggingObjectInterface findExistingLoggingSource(LoggingObjectInterface loggingObject) {
        LoggingObjectInterface found = null;
        for (LoggingObjectInterface verify : this.map.values()) {
            if (loggingObject.equals(verify)) {
                found = verify;
                break;
            }
        }
        return found;
    }

    public LoggingObjectInterface getLoggingObject(String logChannelId) {
        return this.map.get(logChannelId);
    }

    public Map<String, LoggingObjectInterface> getMap() {
        return this.map;
    }

    public List<String> getLogChannelChildren(String parentLogChannelId) {
        if (parentLogChannelId == null) {
            return null;
        }
        List<String> list = getLogChannelChildren(new ArrayList<String>(), parentLogChannelId);
        list.add(parentLogChannelId);
        return list;
    }

    private List<String> getLogChannelChildren(List<String> children, String parentLogChannelId) {
        synchronized (this.syncObject) {
            List<String> list = this.childrenMap.get(parentLogChannelId);
            if (list == null) {
                // Don't do anything, just return the input.
                return children;
            }

            Iterator<String> kids = list.iterator();
            while (kids.hasNext()) {
                String logChannelId = kids.next();

                // Add the children recursively
                getLogChannelChildren(children, logChannelId);

                // Also add the current parent
                children.add(logChannelId);
            }
        }

        return children;
    }

    public Date getLastModificationTime() {
        return this.lastModificationTime;
    }

    public String dump(boolean includeGeneral) {
        StringBuilder out = new StringBuilder(50000);
        for (LoggingObjectInterface o : this.map.values()) {
            if ((includeGeneral) || (!o.getObjectType().equals(LoggingObjectType.GENERAL))) {
                out.append(o.getContainerObjectId());
                out.append("\t");
                out.append(o.getLogChannelId());
                out.append("\t");
                out.append(o.getObjectType().name());
                out.append("\t");
                out.append(o.getObjectName());
                out.append("\t");
                out.append(o.getParent() != null ? o.getParent().getLogChannelId() : "-");
                out.append("\t");
                out.append(o.getParent() != null ? o.getParent().getObjectType().name() : "-");
                out.append("\t");
                out.append(o.getParent() != null ? o.getParent().getObjectName() : "-");
                out.append("\n");
            }
        }
        return out.toString();
    }

    /**
     * For junit testing purposes
     *
     * @return ro items map
     */
    Map<String, LoggingObjectInterface> dumpItems() {
        return Collections.unmodifiableMap(this.map);
    }

    /**
     * For junit testing purposes
     *
     * @return ro parent-child relations map
     */
    Map<String, List<String>> dumpChildren() {
        return Collections.unmodifiableMap(this.childrenMap);
    }

    public void removeIncludingChildren(String logChannelId) {
        synchronized (this.map) {
            List<String> children = getLogChannelChildren(logChannelId);
            for (String child : children) {
                this.map.remove(child);
            }
            this.map.remove(logChannelId);
            removeOrphans();
        }
    }

    public void removeOrphans() {
        // Remove all orphaned children
        this.childrenMap.keySet().retainAll(this.map.keySet());
    }

    public void registerLogChannelFileWriterBuffer(LogChannelFileWriterBuffer fileWriterBuffer) {
        //this.fileWriterBuffers.put(fileWriterBuffer.getLogChannelId(), fileWriterBuffer);
    }

    public LogChannelFileWriterBuffer getLogChannelFileWriterBuffer(String id) {
        /*
        for (String bufferId : this.fileWriterBuffers.keySet()) {
            if (getLogChannelChildren(bufferId).contains(id)) {
                return this.fileWriterBuffers.get(bufferId);
            }
        }
        */
        return null;
    }

    protected List<String> getLogChannelFileWriterBufferIds() {
        /*
        Set<String> bufferIds = this.fileWriterBuffers.keySet();

        List<String> ids = new ArrayList<>();
        for (String id : bufferIds) {
            ids.addAll(getLogChannelChildren(id));
        }

        ids.addAll(bufferIds);
        return ids;
        */
        return EMPTY_LIST;
    }

    public void removeLogChannelFileWriterBuffer(String id) {
        /*
        Set<String> bufferIds = this.fileWriterBuffers.keySet();

        for (String bufferId : bufferIds) {
            if (getLogChannelChildren(id).contains(bufferId)) {
                this.fileWriterBuffers.remove(bufferId);
            }
        }
        */
    }
}
