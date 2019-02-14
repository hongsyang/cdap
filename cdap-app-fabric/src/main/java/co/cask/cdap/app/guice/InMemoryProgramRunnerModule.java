/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package co.cask.cdap.app.guice;

import co.cask.cdap.api.artifact.ArtifactManager;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.app.runtime.ProgramRunnerFactory;
import co.cask.cdap.app.runtime.ProgramRuntimeProvider;
import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.ResolvingDiscoverable;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactFinder;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactManagerFactory;
import co.cask.cdap.internal.app.runtime.artifact.LocalArtifactManager;
import co.cask.cdap.internal.app.runtime.artifact.LocalPluginFinder;
import co.cask.cdap.internal.app.runtime.artifact.PluginFinder;
import co.cask.cdap.internal.app.runtime.batch.MapReduceProgramRunner;
import co.cask.cdap.internal.app.runtime.service.InMemoryProgramRuntimeService;
import co.cask.cdap.internal.app.runtime.service.InMemoryServiceProgramRunner;
import co.cask.cdap.internal.app.runtime.service.ServiceProgramRunner;
import co.cask.cdap.internal.app.runtime.worker.InMemoryWorkerRunner;
import co.cask.cdap.internal.app.runtime.worker.WorkerProgramRunner;
import co.cask.cdap.internal.app.runtime.workflow.WorkflowProgramRunner;
import co.cask.cdap.proto.ProgramType;
import com.google.inject.Inject;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.name.Named;
import org.apache.twill.api.ServiceAnnouncer;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;

import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * Guice more for binding {@link ProgramRunner} that runs program in the same process.
 */
final class InMemoryProgramRunnerModule extends PrivateModule {

  /**
   * Configures a {@link com.google.inject.Binder} via the exposed methods.
   */
  @Override
  protected void configure() {

    // Bind ServiceAnnouncer for service.
    bind(ServiceAnnouncer.class).to(DiscoveryServiceAnnouncer.class);

    // Bind the ArtifactManager implementation and expose it.
    // It could used by ProgramRunner loaded through runtime extension.
    install(new FactoryModuleBuilder()
              .implement(ArtifactManager.class, LocalArtifactManager.class)
              .build(ArtifactManagerFactory.class));
    expose(ArtifactManagerFactory.class);

    bind(PluginFinder.class).to(LocalPluginFinder.class);
    bind(ArtifactFinder.class).to(LocalPluginFinder.class);
    expose(PluginFinder.class);
    expose(ArtifactFinder.class);

    // Bind ProgramRunner
    MapBinder<ProgramType, ProgramRunner> runnerFactoryBinder =
      MapBinder.newMapBinder(binder(), ProgramType.class, ProgramRunner.class);
    // Programs with multiple instances have an InMemoryProgramRunner that starts threads to manage all of their
    // instances.
    runnerFactoryBinder.addBinding(ProgramType.MAPREDUCE).to(MapReduceProgramRunner.class);
    runnerFactoryBinder.addBinding(ProgramType.WORKFLOW).to(WorkflowProgramRunner.class);
    runnerFactoryBinder.addBinding(ProgramType.WORKER).to(InMemoryWorkerRunner.class);
    runnerFactoryBinder.addBinding(ProgramType.SERVICE).to(InMemoryServiceProgramRunner.class);

    // Bind program runners in private scope
    // They should only be used by the ProgramRunners in the runnerFactoryBinder
    bind(ServiceProgramRunner.class);
    bind(WorkerProgramRunner.class);

    // ProgramRunnerFactory should be in local mode
    bind(ProgramRuntimeProvider.Mode.class).toInstance(ProgramRuntimeProvider.Mode.LOCAL);
    bind(ProgramRunnerFactory.class).to(DefaultProgramRunnerFactory.class).in(Scopes.SINGLETON);
    // Note: Expose for test cases. Need to refactor test cases.
    expose(ProgramRunnerFactory.class);

    // Bind and expose runtime service
    bind(ProgramRuntimeService.class).to(InMemoryProgramRuntimeService.class).in(Scopes.SINGLETON);
    expose(ProgramRuntimeService.class);
  }

  @Singleton
  private static final class DiscoveryServiceAnnouncer implements ServiceAnnouncer {

    private final DiscoveryService discoveryService;
    private final InetAddress hostname;

    @Inject
    private DiscoveryServiceAnnouncer(DiscoveryService discoveryService,
                                      @Named(Constants.Service.MASTER_SERVICES_BIND_ADDRESS) InetAddress hostname) {
      this.discoveryService = discoveryService;
      this.hostname = hostname;
    }

    @Override
    public Cancellable announce(final String serviceName, final int port) {
      return discoveryService.register(
        ResolvingDiscoverable.of(new Discoverable(serviceName, new InetSocketAddress(hostname, port))));
    }

    @Override
    public Cancellable announce(String serviceName, int port, byte[] payload) {
      return discoveryService.register(
        ResolvingDiscoverable.of(new Discoverable(serviceName, new InetSocketAddress(hostname, port), payload)));
    }
  }
}
