/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.etl.spark.streaming;

import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.etl.common.NoopStageStatisticsCollector;
import co.cask.cdap.etl.common.StageStatisticsCollector;
import co.cask.cdap.etl.proto.v2.spec.StageSpec;
import co.cask.cdap.etl.spark.function.PluginFunctionContext;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Serializable context that can be used to dynamically instantiate plugins from a driver context.
 * Used for Spark Streaming programs that use checkpoints. Basically just a wrapper around
 * the {@link PluginFunctionContext}, which is used in Spark executor closures. The reason we need this on top of
 * the {@link PluginFunctionContext} is because {@link JavaSparkExecutionContext} can only be used in Spark driver
 * closures and not in spark executor closurers, and we need {@link JavaSparkExecutionContext} to make sure
 * runtime arguments and logical start time are fetched correctly.
 */
public class DynamicDriverContext implements Externalizable {
  private String serializationVersion;
  private StageSpec stageSpec;
  private JavaSparkExecutionContext sec;
  private PluginFunctionContext pluginFunctionContext;

  public DynamicDriverContext() {
    // for deserialization
  }

  public DynamicDriverContext(StageSpec stageSpec, JavaSparkExecutionContext sec, StageStatisticsCollector collector) {
    this.serializationVersion = "4.3";
    this.stageSpec = stageSpec;
    this.sec = sec;
    this.pluginFunctionContext = new PluginFunctionContext(stageSpec, sec, collector);
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeUTF(serializationVersion);
    out.writeObject(stageSpec);
    out.writeObject(sec);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    serializationVersion = in.readUTF();
    stageSpec = (StageSpec) in.readObject();
    sec = (JavaSparkExecutionContext) in.readObject();

    // we intentionally do not serialize this context in order to ensure that the runtime arguments
    // and logical start time are picked up from the JavaSparkExecutionContext. If we serialized it,
    // the arguments and start time of the very first pipeline run would get serialized, then
    // used for every subsequent run that loads from the checkpoint.
    pluginFunctionContext = new PluginFunctionContext(stageSpec, sec, new NoopStageStatisticsCollector());
  }

  public JavaSparkExecutionContext getSparkExecutionContext() {
    return sec;
  }

  public PluginFunctionContext getPluginFunctionContext() {
    return pluginFunctionContext;
  }
}
