/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Pentaho : http://www.pentaho.com
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

package org.pentaho.di.trans;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.ObjectLocationSpecificationMethod;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.util.CurrentDirectoryResolver;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.resource.ResourceDefinitionHelper;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.metastore.api.IMetaStore;

/**
 * This class is supposed to use in steps where the mapping to sub transformations takes place
 *
 * @author Yury Bakhmutski
 * @since 02-jan-2017
 */
public abstract class StepWithMappingMeta extends BaseStepMeta {
    //default value
    private static Class<?> PKG = StepWithMappingMeta.class;

    protected ObjectLocationSpecificationMethod specificationMethod;
    protected String transName;
    protected String fileName;
    protected String directoryPath;
    protected ObjectId transObjectId;

    public static TransMeta loadMappingMeta(StepWithMappingMeta mappingMeta, Repository rep,
                                            IMetaStore metaStore, VariableSpace space) throws KettleException {
        return loadMappingMeta(mappingMeta, rep, metaStore, space, true);
    }

    public static synchronized TransMeta loadMappingMeta(StepWithMappingMeta executorMeta, Repository rep,
                                                         IMetaStore metaStore, VariableSpace space, boolean share) throws KettleException {
        TransMeta mappingTransMeta = null;
        String realFileName = null;

        CurrentDirectoryResolver r = new CurrentDirectoryResolver();
        VariableSpace tmpSpace =
                r.resolveCurrentDirectory(executorMeta.getSpecificationMethod(), space, rep, executorMeta.getParentStepMeta(),
                        executorMeta.getFileName());

        switch (executorMeta.getSpecificationMethod()) {
            case FILENAME:
                realFileName = ResourceDefinitionHelper.normalizeFileName(
                        tmpSpace.environmentSubstitute(executorMeta.getFileName()), r);
                try {
                    // OK, load the meta-data from file...
                    //
                    // Don't set internal variables: they belong to the parent thread!
                    //
                    if (rep != null) {
                        // need to try to load from the repository
                        String dirStr = ResourceDefinitionHelper.extractDirectory(realFileName);
                        String tmpFilename = ResourceDefinitionHelper.extractFileName(realFileName, false);

                        try {
                            RepositoryDirectoryInterface dir = rep.findDirectory(dirStr);
                            if (dir != null) {
                                LogChannel.GENERAL.logDetailed("Loading transformation [" + realFileName + "] from repository...");
                                mappingTransMeta = ResourceDefinitionHelper.loadTransformation(rep, dir, tmpFilename);
                            }
                        } catch (KettleException ke) {
                            LogChannel.GENERAL.logDetailed("Unable to load transformation [" + realFileName + "] from repository", ke);
                            // fall back to try loading from file system (transMeta is going to be null)
                        }
                    }
                    if (mappingTransMeta == null && ResourceDefinitionHelper.fileExists(realFileName)) {
                        LogChannel.GENERAL.logDetailed("Loading transformation from [" + realFileName + "]...");
                        mappingTransMeta = new TransMeta(realFileName, metaStore, rep, true, tmpSpace, null);
                    }
                } catch (Exception e) {
                    throw new KettleException(BaseMessages.getString(PKG, "StepWithMappingMeta.Exception.UnableToLoadTrans"),
                            e);
                }
                break;

            case REPOSITORY_BY_NAME:
                String realDirectory = ResourceDefinitionHelper.normalizeFileName(
                        tmpSpace.environmentSubstitute(executorMeta.getDirectoryPath()), r);
                String realTransName = tmpSpace.environmentSubstitute(executorMeta.getTransName());
                realFileName = realDirectory + '/' + realTransName;

                if (rep != null) {
                    RepositoryDirectoryInterface repositoryDirectory = rep.findDirectory(realDirectory);
                    if (repositoryDirectory != null) {
                        LogChannel.GENERAL.logDetailed("Loading transformation [" + realFileName + "] from repository...");
                        mappingTransMeta = rep.loadTransformation(realTransName, repositoryDirectory, null, true, null);
                    }
                }

                if (mappingTransMeta == null && !ResourceDefinitionHelper.containsVariable(realFileName)) {
                    // rep is null, let's try loading by filename
                    try {
                        LogChannel.GENERAL.logDetailed("Loading transformation from [" + realFileName + "]");
                        mappingTransMeta =
                                new TransMeta(realFileName, metaStore, rep, true, tmpSpace, null);
                    } catch (KettleException ke) {
                        String ext = "." + Const.STRING_TRANS_DEFAULT_EXT;
                        try {
                            // add .ktr extension and try again
                            LogChannel.GENERAL.logDetailed("Try again by loading transformation [" + realFileName + "] with " + ext + " extension");
                            mappingTransMeta =
                                    new TransMeta(realFileName + ext, metaStore, rep, true, tmpSpace, null);
                        } catch (KettleException ke2) {
                            throw new KettleException(BaseMessages.getString(PKG, "StepWithMappingMeta.Exception.UnableToLoadTrans",
                                    realTransName) + realDirectory);
                        }
                    }
                }
                break;

            case REPOSITORY_BY_REFERENCE:
                ObjectId transObjectId = executorMeta.getTransObjectId();
                realFileName = String.valueOf(transObjectId);
                // Read the last revision by reference...
                mappingTransMeta = rep.loadTransformation(transObjectId, null);
                break;
            default:
                break;
        }

        if (mappingTransMeta == null) {
            throw new KettleException("Failed to load transformation [" + realFileName + "]");
        }

        // Pass some important information to the mapping transformation metadata:
        if (share) {
            mappingTransMeta.copyVariablesFrom(space);
        }
        mappingTransMeta.setRepository(rep);
        mappingTransMeta.setMetaStore(metaStore);
        mappingTransMeta.setFilename(mappingTransMeta.getFilename());

        return mappingTransMeta;
    }

    /**
     * @return the specificationMethod
     */
    public ObjectLocationSpecificationMethod getSpecificationMethod() {
        return specificationMethod;
    }

    /**
     * @param specificationMethod the specificationMethod to set
     */
    public void setSpecificationMethod(ObjectLocationSpecificationMethod specificationMethod) {
        this.specificationMethod = specificationMethod;
    }

    /**
     * @return the directoryPath
     */
    public String getDirectoryPath() {
        return directoryPath;
    }

    /**
     * @param directoryPath the directoryPath to set
     */
    public void setDirectoryPath(String directoryPath) {
        this.directoryPath = directoryPath;
    }

    /**
     * @return the fileName
     */
    public String getFileName() {
        return fileName;
    }

    /**
     * @param fileName the fileName to set
     */
    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    /**
     * @return the transName
     */
    public String getTransName() {
        return transName;
    }

    /**
     * @param transName the transName to set
     */
    public void setTransName(String transName) {
        this.transName = transName;
    }

    /**
     * @return the transObjectId
     */
    public ObjectId getTransObjectId() {
        return transObjectId;
    }

    /**
     * @param transObjectId the transObjectId to set
     */
    public void setTransObjectId(ObjectId transObjectId) {
        this.transObjectId = transObjectId;
    }

}
