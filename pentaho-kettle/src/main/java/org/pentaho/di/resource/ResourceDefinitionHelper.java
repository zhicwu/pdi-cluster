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
package org.pentaho.di.resource;

import org.apache.commons.vfs2.FileObject;
import org.pentaho.di.base.AbstractMeta;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.util.EnvUtil;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.repository.*;
import org.pentaho.di.trans.TransMeta;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Utility class for loading job and transformation meta data.
 *
 * @author Zhichun Wu
 */
public final class ResourceDefinitionHelper {
    private final static Class<?> PKG = JobMeta.class; // for i18n purposes, needed by Translator2!!

    // FIXME this assumes kettle.properties has been loaded, which might not always true...
    private static final boolean KEEP_EXPORTED_FILE = "Y".equalsIgnoreCase(
            EnvUtil.getSystemProperty("KETTLE_KEEP_EXPORTED_FILE", "N"));

    private final static String VARIABLE_PREFIX = "${";
    private final static String VARIABLE_SUFFIX = "}";

    public static class TransMetaCollection extends TransMeta {
        private final List<TransMeta> attachedMeta = new ArrayList<>();

        public void attachTransMeta(TransMeta transMeta) {
            attachedMeta.add(transMeta);
        }

        public List<TransMeta> getAttachedMeta() {
            return attachedMeta;
        }
    }

    public static class JobMetaCollection extends JobMeta {
        private final List<JobMeta> attachedMeta = new ArrayList<>();

        public void attachJobMeta(JobMeta jobMeta) {
            attachedMeta.add(jobMeta);
        }

        public List<JobMeta> getAttachedMeta() {
            return attachedMeta;
        }
    }

    public static void purge(FileObject tempFile) {
        if (!KEEP_EXPORTED_FILE && tempFile != null) {
            try {
                tempFile.delete();
            } catch (Exception e) {
                // pretend nothing happened
            }
        }
    }

    public static boolean containsVariable(String name) {
        boolean hasVar = false;

        if (name != null) {
            int index = name.indexOf(VARIABLE_PREFIX);
            // variable name should at least contain one character
            index = index >= 0 ? name.indexOf(VARIABLE_SUFFIX, index + VARIABLE_PREFIX.length() + 1) : -1;

            hasVar = index > 0;
        }

        return hasVar;
    }

    public static boolean containsResource(Repository repository,
                                           Map<String, ResourceDefinition> definitions,
                                           VariableSpace space,
                                           ResourceNamingInterface namingInterface,
                                           AbstractMeta meta) throws KettleException {
        if (definitions == null || space == null || namingInterface == null || meta == null) {
            return false;
        }

        String extension = meta instanceof TransMeta ? Const.STRING_TRANS_DEFAULT_EXT : Const.STRING_JOB_DEFAULT_EXT;
        String fullname;
        try {
            RepositoryDirectoryInterface directory = meta.getRepositoryDirectory();
            if (Const.isEmpty(meta.getFilename())) {
                // Assume repository...
                //
                fullname =
                        directory.getPath() + (directory.getPath().endsWith(RepositoryDirectory.DIRECTORY_SEPARATOR) ? ""
                                : RepositoryDirectory.DIRECTORY_SEPARATOR) + meta.getName() + "." + extension; //
            } else {
                // Assume file
                //
                FileObject fileObject = KettleVFS.getFileObject(space.environmentSubstitute(meta.getFilename()), space);
                fullname = fileObject.getName().getPath();
            }
        } catch (KettleFileException e) {
            throw new KettleException(
                    BaseMessages.getString(PKG, "JobMeta.Exception.AnErrorOccuredReadingJob", meta.getFilename()), e);
        }

        // if (repository != null) {
        //    repository.getLog().logError("=====> Checking [" + fullname + "] in " + definitions + " result=" + definitions.containsKey(fullname));
        // }
        return definitions.containsKey(fullname) || meta.equals(space);
    }

    public static TransMeta loadTransformation(
            Repository rep, RepositoryDirectoryInterface dir, String realFileName) throws KettleException {
        TransMeta transMeta = null;
        if (rep == null || dir == null || realFileName == null) {
            return transMeta;
        }

        // rep.getLog().logError("=====> Loading Trans[" + realFileName + "], contains variable=" + containsVariable(realFileName));
        if (containsVariable(realFileName)) {
            TransMetaCollection tmc = new TransMetaCollection();
            transMeta = tmc;
            transMeta.setFilename(realFileName);
            for (RepositoryElementMetaInterface element : dir.getRepositoryObjects()) {
                if (element.getObjectType() != RepositoryObjectType.TRANSFORMATION) {
                    continue;
                }

                tmc.attachTransMeta(rep.loadTransformation(element.getName(), dir, null, true, null));
            }
        } else {
            transMeta = rep.loadTransformation(realFileName, dir, null, true, null);
        }

        return transMeta;
    }

    public static JobMeta loadJob(
            Repository rep, RepositoryDirectoryInterface dir, String realFileName) throws KettleException {
        JobMeta jobMeta = null;
        if (rep == null || dir == null || realFileName == null) {
            return jobMeta;
        }

        // rep.getLog().logError("=====> Loading Job[" + realFileName + "], contains variable=" + containsVariable(realFileName));
        if (containsVariable(realFileName)) {
            JobMetaCollection jmc = new JobMetaCollection();
            jobMeta = jmc;
            jobMeta.setFilename(realFileName);
            for (RepositoryElementMetaInterface element : dir.getRepositoryObjects()) {
                if (element.getObjectType() != RepositoryObjectType.JOB) {
                    continue;
                }

                jmc.attachJobMeta(rep.loadJob(element.getName(), dir, null, null));
            }
        } else {
            jobMeta = rep.loadJob(realFileName, dir, null, null);
        }

        return jobMeta;
    }

    private ResourceDefinitionHelper() {
    }
}
