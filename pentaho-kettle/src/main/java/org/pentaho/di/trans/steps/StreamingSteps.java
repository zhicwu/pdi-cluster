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
package org.pentaho.di.trans.steps;

import org.pentaho.di.trans.step.StepIOMetaInterface;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.step.errorhandling.StreamInterface;

import java.util.ArrayList;
import java.util.List;

/**
 * This class represents names of input / output steps.
 *
 * @author Zhichun Wu
 */
public final class StreamingSteps {
    private final String[] stepNames;

    public StreamingSteps(StepMetaInterface stepMeta) {
        this(stepMeta, StreamInterface.StreamType.INFO);
    }

    public StreamingSteps(StepMetaInterface stepMeta, StreamInterface.StreamType streamType) {
        StepIOMetaInterface stepIOMeta = stepMeta == null ? null : stepMeta.getStepIOMeta();
        List<StreamInterface> streams = stepIOMeta == null
                ? null : (streamType == StreamInterface.StreamType.OUTPUT
                ? stepIOMeta.getTargetStreams() : stepIOMeta.getInfoStreams());

        if (streams == null) {
            streams = new ArrayList<StreamInterface>(0);
        }

        stepNames = new String[streams.size()];
        for (int i = 0; i < stepNames.length; i++) {
            String name = (String) streams.get(i).getSubject();
            stepNames[i] = name == null ? streams.get(i).getStepname() : name;
        }
    }

    public String getStepName() {
        return getStepName(0);
    }

    public String getStepName(int index) {
        return (index < 0 || index >= stepNames.length) ? null : stepNames[index];
    }
}
