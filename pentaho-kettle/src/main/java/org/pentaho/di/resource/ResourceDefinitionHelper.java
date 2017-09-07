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

import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.vfs2.FileObject;
import org.pentaho.di.base.AbstractMeta;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.util.CurrentDirectoryResolver;
import org.pentaho.di.core.util.EnvUtil;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.repository.*;
import org.pentaho.di.trans.TransMeta;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

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

    private final static String TRANS_FILE_EXT = "." + Const.STRING_TRANS_DEFAULT_EXT;
    private final static String JOB_FILE_EXT = "." + Const.STRING_JOB_DEFAULT_EXT;

    private final static String CLASS_SIMPLE_REPOSITORY_FILE_DATA
            = "org.pentaho.platform.api.repository2.unified.data.simple.SimpleRepositoryFileData";
    private final static String METHOD_GET_DATA = "getDataForRead";
    private final static String METHOD_GET_ENCODING = "getEncoding";
    private final static String METHOD_GET_FILE = "getFile";
    private final static String METHOD_GET_ID = "getId";
    private final static String METHOD_GET_INPUTSTREAM = "getInputStream";
    private final static String METHOD_GET_NAME = "getName";
    private final static String METHOD_GET_PUR = "getPur";

    private final static String FS_SCHEMA = "file";
    private final static String FS_PROTOCOL = FS_SCHEMA + "://";

    private final static String WARN_FAILED_TO_LOAD_FILE = "Failed to get file content from Pentaho Repository: ";

    public static class SimpleFileMeta {
        private final String fileName;
        private final boolean isText;
        private final String textContent;
        private final byte[] binaryContent;
        private final boolean available;

        public SimpleFileMeta(String fileName, boolean isText,
                              String textContent, byte[] binaryContent, boolean available) {
            this.fileName = fileName;
            this.isText = isText;
            this.textContent = textContent;
            this.binaryContent = binaryContent;
            this.available = available;
        }

        public String getFileName() {
            return fileName;
        }

        public boolean isText() {
            return isText;
        }

        public boolean isBinary() {
            return !isText;
        }

        public String getTextContent() {
            return textContent;
        }

        public byte[] getBinaryContent() {
            return binaryContent;
        }

        public InputStream getBinaryInputStream() {
            return new ByteArrayInputStream(binaryContent);
        }

        public boolean isAvailable() {
            return available;
        }
    }

    public static class TransMetaCollection extends TransMeta {
        private final List<TransMeta> attachedMeta = new ArrayList<>();

        public void attachTransMeta(TransMeta transMeta) {
            attachedMeta.add(transMeta);
        }

        public List<TransMeta> getAttachedMeta() {
            return attachedMeta;
        }

        @Override
        public void clear() {
            if (this.attachedMeta != null) {
                this.attachedMeta.clear();
            }

            super.clear();
        }

        @Override
        public Object realClone(boolean doClear) {
            TransMetaCollection tmc = new TransMetaCollection();
            tmc.setName(this.getName());
            tmc.setFilename(this.getFilename());
            if (doClear) {
                this.clear();
            }

            for (TransMeta tm : attachedMeta) {
                tmc.attachedMeta.add((TransMeta) tm.realClone(doClear));
            }

            return tmc;
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

        @Override
        public void clear() {
            if (this.attachedMeta != null) {
                this.attachedMeta.clear();
            }

            super.clear();
        }

        @Override
        public Object realClone(boolean doClear) {
            JobMetaCollection jmc = new JobMetaCollection();
            jmc.setName(this.getName());
            jmc.setFilename(this.getFilename());
            if (doClear) {
                this.clear();
            }

            for (JobMeta jm : attachedMeta) {
                jmc.attachedMeta.add((JobMeta) jm.realClone(doClear));
            }

            return jmc;
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
            if (Utils.isEmpty(meta.getFilename())) {
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

    private static void loadTransformationRecursively(
            Repository rep, TransMetaCollection tmc, RepositoryDirectoryInterface dir) throws KettleException {
        if (rep == null || tmc == null || dir == null) {
            return;
        }

        for (RepositoryElementMetaInterface element : dir.getRepositoryObjects()) {
            if (element.getObjectType() != RepositoryObjectType.TRANSFORMATION) {
                continue;
            }

            // rep.getLog().logError("=====> Loading Trans[" + element.getName() + "] from same directory: " + dir);
            tmc.attachTransMeta(rep.loadTransformation(element.getName(), dir, null, true, null));
        }

        // now sub-directories
        for (RepositoryDirectoryInterface d : dir.getChildren()) {
            loadTransformationRecursively(rep, tmc, d);
        }
    }

    public static TransMeta loadTransformation(
            Repository rep, RepositoryDirectoryInterface dir, String realFileName) throws KettleException {
        if (rep == null || dir == null || realFileName == null) {
            return null;
        }

        String key = dir.getPathObjectCombination(realFileName);
        // rep.getLog().logError("=====> Loading Trans[" + key + "], contains variable=" + containsVariable(realFileName));
        TransMeta transMeta = null;
        if (containsVariable(realFileName)) {
            TransMetaCollection tmc = new TransMetaCollection();
            transMeta = tmc;
            transMeta.setFilename(realFileName);

            loadTransformationRecursively(rep, tmc, dir);
        } else {
            int idx = realFileName.indexOf(TRANS_FILE_EXT);
            if (idx > 0) { // try without extension
                try {
                    transMeta = rep.loadTransformation(realFileName.substring(0, idx),
                            dir, null, true, null);
                } catch (KettleException e) {
                    // ignore exception
                }
            }

            if (transMeta == null) {
                transMeta = rep.loadTransformation(realFileName, dir, null, true, null);
            }
        }

        return transMeta;
    }

    private static void loadJobRecursively(
            Repository rep, JobMetaCollection jmc, RepositoryDirectoryInterface dir) throws KettleException {
        if (rep == null || jmc == null || dir == null) {
            return;
        }

        for (RepositoryElementMetaInterface element : dir.getRepositoryObjects()) {
            if (element.getObjectType() != RepositoryObjectType.JOB) {
                continue;
            }

            // rep.getLog().logError("=====> Loading Job[" + element.getName() + "] from same directory: " + dir);
            jmc.attachJobMeta(rep.loadJob(element.getName(), dir, null, null));
        }

        // now sub-directories
        for (RepositoryDirectoryInterface d : dir.getChildren()) {
            loadJobRecursively(rep, jmc, d);
        }
    }

    public static JobMeta loadJob(
            Repository rep, RepositoryDirectoryInterface dir, String realFileName) throws KettleException {
        if (rep == null || dir == null || realFileName == null) {
            return null;
        }

        String key = dir.getPathObjectCombination(realFileName);
        // rep.getLog().logError("=====> Loading Job[" + key + "], contains variable=" + containsVariable(realFileName));
        JobMeta jobMeta = null;

        if (containsVariable(realFileName)) {
            JobMetaCollection jmc = new JobMetaCollection();
            jobMeta = jmc;
            jobMeta.setFilename(realFileName);

            loadJobRecursively(rep, jmc, dir);
        } else {
            int idx = realFileName.indexOf(JOB_FILE_EXT);
            if (idx > 0) { // try without extension
                try {
                    jobMeta = rep.loadJob(realFileName.substring(0, idx), dir, null, null);
                } catch (KettleException e) {
                    // ignore exception
                }
            }

            if (jobMeta == null) {
                jobMeta = rep.loadJob(realFileName, dir, null, null);
            }
        }

        return jobMeta;
    }

    public static boolean isPentahoRepository(Repository repository) {
        boolean isPur = false;

        if (repository != null) {
            try {
                Class repositoryClass = repository.getClass();
                isPur = repositoryClass.getMethod(METHOD_GET_PUR) != null;
            } catch (Exception e) {
                // ignore errors
            }
        }

        return isPur;
    }

    /**
     * Try to get content of given file(text or binary) from Pentaho Repository.
     *
     * @param repository repository instance
     * @param fileName   substituted file name
     * @param isTextFile true if it's a text file; false otherwise
     * @param logger     logger interface
     * @return an object contains name, type and content of the file
     */
    public static SimpleFileMeta loadFileFromPurRepository(Repository repository,
                                                           String fileName,
                                                           boolean isTextFile,
                                                           LogChannelInterface logger) {
        String simpleName = FilenameUtils.normalize(fileName);
        String textContent = "";
        byte[] binaryContent = new byte[0];

        InputStream is = null;
        boolean success = false;
        try {
            Class repositoryClass = repository.getClass();
            Object unifiedRepository = repositoryClass.getMethod(METHOD_GET_PUR).invoke(repository);

            Class unifiedRepositoryClass = unifiedRepository.getClass();
            Object repositoryFile = unifiedRepositoryClass.getMethod(METHOD_GET_FILE, String.class)
                    .invoke(unifiedRepository, simpleName);

            Class repositoryFileClass = repositoryFile.getClass();
            Object fileId = repositoryFileClass.getMethod(METHOD_GET_ID).invoke(repositoryFile);

            simpleName = (String) repositoryFileClass.getMethod(METHOD_GET_NAME).invoke(repositoryFile);

            Object fileData = unifiedRepositoryClass.getMethod(METHOD_GET_DATA, Serializable.class, Class.class)
                    .invoke(unifiedRepository, fileId,
                            unifiedRepositoryClass.getClassLoader().loadClass(CLASS_SIMPLE_REPOSITORY_FILE_DATA));
            Class fileDataClass = fileData.getClass();
            is = (InputStream) fileDataClass.getMethod(METHOD_GET_INPUTSTREAM).invoke(fileData);

            if (isTextFile) {
                String encoding = null;
                // just try to get file encoding if we could
                try {
                    encoding = (String) repositoryFileClass.getMethod(METHOD_GET_ENCODING).invoke(repositoryFile);
                } catch (Exception ex) {
                }

                textContent = IOUtils.toString(is, encoding);
            } else {
                // FIXME this could be a problem, let's hope the binary file is not that large...
                binaryContent = IOUtils.toByteArray(is);
            }

            success = true;
        } catch (NoSuchMethodException | SecurityException | NullPointerException e) {
            if (logger != null && logger.isDebug()) {
                logger.logDebug(WARN_FAILED_TO_LOAD_FILE + fileName, e);
            }
        } catch (Exception e) {
            if (logger != null && logger.isError()) {
                logger.logError(WARN_FAILED_TO_LOAD_FILE + fileName, e);
            }
        } finally {
            IOUtils.closeQuietly(is);
        }

        return new SimpleFileMeta(simpleName, isTextFile, textContent, binaryContent, success);
    }

    /**
     * Try to get content of given text file from Pentaho Repository.
     *
     * @param repository repository instance
     * @param fileName   substituted file name
     * @param logger     logger interface
     * @return text file content
     */
    public static String getTextFileContent(Repository repository, String fileName, LogChannelInterface logger) {
        return loadFileFromPurRepository(repository, fileName, true, logger).getTextContent();
    }

    public static String normalizeFileName(String fileName, VariableSpace space) {
        if (space != null) {
            fileName = space.environmentSubstitute(fileName);
        }

        return normalizeFileName(fileName);
    }

    public static String normalizeFileName(String fileName, CurrentDirectoryResolver resolver) {
        String normalizedFileName = normalizeFileName(fileName);

        URI uri = null;
        try {
            uri = new URI(normalizedFileName);
        } catch (URISyntaxException e) {
            // ignore what happened
        }

        if (uri == null || Strings.isNullOrEmpty(uri.getScheme()) || FS_SCHEMA.equalsIgnoreCase(uri.getScheme())) {
            normalizedFileName = uri.getPath(); // this also removes parameters in URI, which is good
            if (resolver != null) {
                normalizedFileName = resolver.normalizeSlashes(normalizedFileName);
            }
        }

        return normalizedFileName;
    }

    public static String normalizeFileName(String fileName) {
        String normalizedFileName = FilenameUtils.normalize(fileName);
        if (!Strings.isNullOrEmpty(normalizedFileName)) {
            fileName = normalizedFileName;
        }

        return fileName;
    }

    public static String extractDirectory(String fileName) {
        if (fileName == null) {
            return fileName;
        }

        // FIXME still possible that fileName does not contain any variable
        int varIdx = fileName.indexOf(VARIABLE_PREFIX);
        int pathIdx = fileName.lastIndexOf('/');

        if (varIdx > 0) {
            for (int i = varIdx - 1; i >= 0; i--) {
                if (fileName.charAt(i) == '/') {
                    pathIdx = i;
                    break;
                }
            }
        }

        return pathIdx > 0 ? fileName.substring(0, pathIdx) : "/";
    }

    public static String extractFileName(String fileName, boolean underRoot) {
        fileName = FilenameUtils.getName(fileName);
        return underRoot ? new StringBuilder().append('/').append(fileName).toString() : fileName;
    }

    public static String extractExtension(String fileName) {
        return extractExtension(fileName, false);
    }

    public static String extractExtension(String fileName, boolean withDot) {
        StringBuilder sb = new StringBuilder(10);
        if (withDot) {
            sb.append('.');
        }

        if (fileName != null) {
            sb.append(FilenameUtils.getExtension(fileName));
        }

        return sb.toString();
    }

    public static String normalizeJobResourceName(String resourceName) {
        return resourceName.replace(FS_PROTOCOL,
                new StringBuilder(20)
                        .append(VARIABLE_PREFIX)
                        .append(Const.INTERNAL_VARIABLE_JOB_FILENAME_DIRECTORY)
                        .append(VARIABLE_SUFFIX)
                        .toString());
    }

    public static String normalizeTransformationResourceName(String resourceName) {
        return resourceName.replace(FS_PROTOCOL,
                new StringBuilder(20)
                        .append(VARIABLE_PREFIX)
                        .append(Const.INTERNAL_VARIABLE_TRANSFORMATION_FILENAME_DIRECTORY)
                        .append(VARIABLE_SUFFIX)
                        .toString());
    }

    public static String generateNewFileNameForOutput(String fileName, boolean fromPur) {
        if (fileName == null) {
            return fileName;
        }

        if (fromPur) {
            fileName = extractFileName(fileName, false);
        }

        String slash = "/";
        int index = fileName.indexOf('!');

        if (index > 0) {
            // remove duplicated '/'
            String pattern = slash + "+";

            fileName = fileName.substring(index + 1).replaceAll(pattern, slash);
            if (fileName.startsWith(slash)) {
                fileName = fileName.substring(1);
            }

            fromPur = true;
        }

        if (fromPur) {
            String tmpDir = FilenameUtils.normalize(System.getProperty("java.io.tmpdir")).replace('\\', '/');

            StringBuilder sb = new StringBuilder();
            sb.append(tmpDir);
            if (sb.length() == 0 || sb.charAt(sb.length() - 1) != slash.charAt(0)) {
                sb.append(slash);
            }
            sb.append(fileName);
            fileName = sb.toString();
        }

        return fileName;
    }

    private ResourceDefinitionHelper() {
    }
}
