/*
 * Copyright © 2016-2019 Cask Data, Inc.
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
package co.cask.cdap.data.runtime.preview;

import co.cask.cdap.data.runtime.DataFabricLevelDBModule;
import co.cask.cdap.data2.datafabric.dataset.RemoteDatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetDefinitionRegistryFactory;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DefaultDatasetDefinitionRegistryFactory;
import co.cask.cdap.data2.dataset2.preview.PreviewDatasetFramework;
import co.cask.cdap.data2.metadata.lineage.DefaultLineageStoreReader;
import co.cask.cdap.data2.metadata.lineage.LineageStoreReader;
import co.cask.cdap.data2.metadata.lineage.field.DefaultFieldLineageReader;
import co.cask.cdap.data2.metadata.lineage.field.FieldLineageReader;
import co.cask.cdap.data2.metadata.writer.FieldLineageWriter;
import co.cask.cdap.data2.metadata.writer.LineageWriter;
import co.cask.cdap.data2.metadata.writer.NoOpLineageWriter;
import co.cask.cdap.data2.registry.NoOpUsageRegistry;
import co.cask.cdap.data2.registry.UsageRegistry;
import co.cask.cdap.data2.registry.UsageWriter;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import co.cask.cdap.spi.metadata.MetadataStorage;
import co.cask.cdap.spi.metadata.dataset.DatasetMetadataStorage;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import com.google.inject.util.Modules;
import org.apache.tephra.TransactionSystemClient;

/**
 * Data fabric modules for preview
 */
public class PreviewDataModules {
  public static final String BASE_DATASET_FRAMEWORK = "basicDatasetFramework";

  public Module getDataFabricModule(final TransactionSystemClient transactionSystemClient) {
    return Modules.override(new DataFabricLevelDBModule()).with(new AbstractModule() {
      @Override
      protected void configure() {
        // Use the distributed version of the transaction client
        bind(TransactionSystemClient.class).toInstance(transactionSystemClient);
      }
    });
  }

  public Module getDataSetsModule(final DatasetFramework remoteDatasetFramework) {

    return new PrivateModule() {
      @Override
      protected void configure() {
        bind(DatasetDefinitionRegistryFactory.class)
          .to(DefaultDatasetDefinitionRegistryFactory.class).in(Scopes.SINGLETON);

        bind(MetadataStorage.class).to(DatasetMetadataStorage.class);
        expose(MetadataStorage.class);

        bind(DatasetFramework.class)
          .annotatedWith(Names.named("localDatasetFramework"))
          .to(RemoteDatasetFramework.class);

        bind(DatasetFramework.class).annotatedWith(Names.named("actualDatasetFramework")).
          toInstance(remoteDatasetFramework);

        bind(DatasetFramework.class).
          annotatedWith(Names.named(BASE_DATASET_FRAMEWORK)).
          toProvider(PreviewDatasetFrameworkProvider.class).in(Scopes.SINGLETON);

        bind(DatasetFramework.class).
          toProvider(PreviewDatasetFrameworkProvider.class).in(Scopes.SINGLETON);
        expose(DatasetFramework.class);

        bind(LineageStoreReader.class).to(DefaultLineageStoreReader.class);
        // Need to expose LineageStoreReader as it's being used by the LineageHandler (through LineageAdmin)
        expose(LineageStoreReader.class);

        bind(FieldLineageReader.class).to(DefaultFieldLineageReader.class);
        expose(FieldLineageReader.class);

        bind(LineageWriter.class).to(NoOpLineageWriter.class);
        expose(LineageWriter.class);

        bind(FieldLineageWriter.class).to(NoOpLineageWriter.class);
        expose(FieldLineageWriter.class);

        bind(UsageWriter.class).to(NoOpUsageRegistry.class).in(Scopes.SINGLETON);
        expose(UsageWriter.class);

        bind(UsageRegistry.class).to(NoOpUsageRegistry.class).in(Scopes.SINGLETON);
        expose(UsageRegistry.class);
      }
    };
  }

  private static final class PreviewDatasetFrameworkProvider implements Provider<DatasetFramework> {
    private final DatasetFramework inMemoryDatasetFramework;
    private final DatasetFramework remoteDatasetFramework;
    private final AuthenticationContext authenticationContext;
    private final AuthorizationEnforcer authorizationEnforcer;

    @Inject
    PreviewDatasetFrameworkProvider(@Named("localDatasetFramework")DatasetFramework inMemoryDatasetFramework,
                                    @Named("actualDatasetFramework")DatasetFramework remoteDatasetFramework,
                                    AuthenticationContext authenticationContext,
                                    AuthorizationEnforcer authorizationEnforcer) {
      this.inMemoryDatasetFramework = inMemoryDatasetFramework;
      this.remoteDatasetFramework = remoteDatasetFramework;
      this.authenticationContext = authenticationContext;
      this.authorizationEnforcer = authorizationEnforcer;
    }

    @Override
    public DatasetFramework get() {
      return new PreviewDatasetFramework(inMemoryDatasetFramework, remoteDatasetFramework,
                                         authenticationContext, authorizationEnforcer);
    }
  }
}
