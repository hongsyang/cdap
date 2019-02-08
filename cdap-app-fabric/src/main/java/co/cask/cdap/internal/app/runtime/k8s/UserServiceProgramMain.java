/*
 * Copyright © 2019 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.k8s;

import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.artifact.ArtifactScope;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.plugin.Plugin;
import co.cask.cdap.api.service.Service;
import co.cask.cdap.app.guice.DistributedArtifactManagerModule;
import co.cask.cdap.app.guice.UnsupportedExploreClient;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.program.ProgramDescriptor;
import co.cask.cdap.app.program.Programs;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.app.runtime.ProgramStateWriter;
import co.cask.cdap.common.ArtifactNotFoundException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.ResolvingDiscoverable;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.common.namespace.guice.NamespaceQueryAdminModule;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.data.runtime.DataSetServiceModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data2.audit.AuditModule;
import co.cask.cdap.explore.client.ExploreClient;
import co.cask.cdap.internal.app.ApplicationSpecificationAdapter;
import co.cask.cdap.internal.app.program.MessagingProgramStateWriter;
import co.cask.cdap.internal.app.runtime.AbstractListener;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.ProgramRunners;
import co.cask.cdap.internal.app.runtime.SimpleProgramOptions;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactFinder;
import co.cask.cdap.internal.app.runtime.codec.ArgumentsCodec;
import co.cask.cdap.internal.app.runtime.codec.ProgramOptionsCodec;
import co.cask.cdap.internal.app.runtime.service.ServiceProgramRunner;
import co.cask.cdap.internal.app.runtime.workflow.MessagingWorkflowStateWriter;
import co.cask.cdap.internal.app.runtime.workflow.WorkflowStateWriter;
import co.cask.cdap.master.environment.k8s.AbstractServiceMain;
import co.cask.cdap.messaging.guice.MessagingClientModule;
import co.cask.cdap.metadata.MetadataReaderWriterModules;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.security.auth.context.AuthenticationContextModules;
import co.cask.cdap.security.authorization.AuthorizationEnforcementModule;
import co.cask.cdap.security.guice.SecureStoreClientModule;
import co.cask.cdap.security.impersonation.CurrentUGIProvider;
import co.cask.cdap.security.impersonation.NoOpOwnerAdmin;
import co.cask.cdap.security.impersonation.OwnerAdmin;
import co.cask.cdap.security.impersonation.UGIProvider;
import com.google.common.base.Throwables;
import com.google.common.io.Closeables;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import org.apache.twill.api.ServiceAnnouncer;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;

/**
 * Run a {@link Service} in Kubernetes.
 */
public class UserServiceProgramMain extends AbstractServiceMain<ServiceOptions> {
  private static final Logger LOG = LoggerFactory.getLogger(UserServiceProgramMain.class);
  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder())
    .registerTypeAdapter(ProgramOptions.class, new ProgramOptionsCodec())
    .registerTypeAdapter(Arguments.class, new ArgumentsCodec())
    .create();

  private Program program;
  private ProgramOptions programOptions;
  private ProgramRunner programRunner;
  private ProgramRunId programRunId;
  private CompletableFuture<ProgramController> controllerFuture;
  private CompletableFuture<ProgramController.State> programCompletion;
  private long maxStopSeconds;

  /**
   * Main entry point
   */
  public static void main(String[] args) throws Exception {
    main(UserServiceProgramMain.class, args);
  }

  @Override
  protected List<Module> getServiceModules() {
    return Arrays.asList(
      new NamespaceQueryAdminModule(),
      new MessagingClientModule(),
      new AuditModule(),
      new AuthorizationEnforcementModule().getDistributedModules(),
      new SecureStoreClientModule(),
      new MetadataReaderWriterModules().getDistributedModules(),
      new DistributedArtifactManagerModule(),
      new AuthenticationContextModules().getProgramContainerModule(),
      // Always use local table implementations, which use LevelDB.
      // In K8s, there won't be HBase and the cdap-site should be set to use SQL store for StructuredTable.
      new DataSetServiceModules().getStandaloneModules(),
      // The Dataset set modules are only needed to satisfy dependency injection
      new DataSetsModules().getDistributedModules(),
      getDataFabricModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(ProgramStateWriter.class).to(MessagingProgramStateWriter.class);
          bind(WorkflowStateWriter.class).to(MessagingWorkflowStateWriter.class);

          // don't need to perform any impersonation from within user programs
          bind(UGIProvider.class).to(CurrentUGIProvider.class).in(Scopes.SINGLETON);

          bind(ServiceAnnouncer.class).toInstance(new ServiceAnnouncer() {
            private final DiscoveryService discoveryService = masterEnv.getDiscoveryServiceSupplier().get();
            private final String hostname = InetAddress.getLoopbackAddress().getCanonicalHostName();

            @Override
            public Cancellable announce(String serviceName, int port) {
              return discoveryService.register(
                ResolvingDiscoverable.of(new Discoverable(serviceName, new InetSocketAddress(hostname, port))));
            }

            @Override
            public Cancellable announce(String serviceName, int port, byte[] payload) {
              return discoveryService.register(
                ResolvingDiscoverable.of(new Discoverable(serviceName, new InetSocketAddress(hostname, port),
                                                          payload)));
            }
          });

          bind(ExploreClient.class).to(UnsupportedExploreClient.class);
          bind(OwnerAdmin.class).to(NoOpOwnerAdmin.class);
        }
      }
    );
  }

  @Override
  protected void addServices(Injector injector, List<? super com.google.common.util.concurrent.Service> services,
                             List<? super AutoCloseable> closeableResources) {
    services.add(injector.getInstance(MetricsCollectionService.class));
  }

  @Nullable
  @Override
  protected LoggingContext getLoggingContext() {
    return null;
  }

  @Override
  protected ServiceOptions createOptions() {
    return new ServiceOptions();
  }

  @Override
  public void init(String[] args) throws MalformedURLException {
    super.init(args);

    ApplicationSpecification appSpec = readFile(new File(options.getAppSpecPath()), ApplicationSpecification.class);
    programOptions = readFile(new File(options.getProgramOptionsPath()), ProgramOptions.class);
    programRunId = programOptions.getProgramId().run(ProgramRunners.getRunId(programOptions));

    controllerFuture = new CompletableFuture<>();
    programCompletion = new CompletableFuture<>();
    maxStopSeconds = cConf.getLong(Constants.AppFabric.PROGRAM_MAX_STOP_SECONDS, 60);

    programRunner = injector.getInstance(ServiceProgramRunner.class);

    // fetch the app artifact
    ArtifactFinder artifactFinder = injector.getInstance(ArtifactFinder.class);

    NamespaceId programNamespace = programRunId.getNamespaceId();
    Set<co.cask.cdap.api.artifact.ArtifactId> pluginArtifacts = new HashSet<>();
    for (Plugin plugin : appSpec.getPlugins().values()) {
      pluginArtifacts.add(plugin.getArtifactId());
    }

    // need to set the plugins directory in system args.
    File pluginsDir = new File(options.getTmpDir(), "plugins");
    Map<String, String> systemArgs = new HashMap<>(programOptions.getArguments().asMap());
    systemArgs.put(ProgramOptionConstants.PLUGIN_DIR, pluginsDir.getAbsolutePath());
    programOptions = new SimpleProgramOptions(programRunId.getParent(), new BasicArguments(systemArgs),
                                              programOptions.getUserArguments());

    try {
      File programDir = new File(options.getTmpDir(), "program");
      DirUtils.mkdirs(programDir);
      File programJarFile = new File(programDir, "program.jar");
      Location programJarLocation =
        artifactFinder.getArtifactLocation(convert(programNamespace, appSpec.getArtifactId()));
      Files.copy(programJarLocation.getInputStream(), programJarFile.toPath());

      DirUtils.mkdirs(pluginsDir);
      for (co.cask.cdap.api.artifact.ArtifactId artifactId : pluginArtifacts) {
        Location location = artifactFinder.getArtifactLocation(convert(programNamespace, artifactId));
        File destFile = new File(pluginsDir, String.format("%s-%s-%s.jar", artifactId.getScope().name(),
                                                           artifactId.getName(), artifactId.getVersion().getVersion()));
        Files.copy(location.getInputStream(), destFile.toPath());
      }
      programRunner = injector.getInstance(ServiceProgramRunner.class);

      // Create the Program instance
      programJarLocation = Locations.toLocation(programJarFile);
      // Unpack the JAR file
      BundleJarUtil.unJar(programJarLocation, programDir);
      program = Programs.create(injector.getInstance(CConfiguration.class), programRunner,
                                new ProgramDescriptor(programOptions.getProgramId(), appSpec), programJarLocation,
                                programDir);
    } catch (ArtifactNotFoundException e) {
      throw new RuntimeException("Unable to find application artifact " + appSpec.getArtifactId(), e);
    } catch (IOException e) {
      Throwables.propagate(e);
    }
  }

  @Override
  public void start() {
    startServices();

    try {
      LOG.info("Starting program run {}", programRunId);

      // Start the program.
      ProgramController controller = programRunner.run(program, programOptions);
      controller.addListener(new AbstractListener() {
        @Override
        public void init(ProgramController.State currentState, @Nullable Throwable cause) {
          switch (currentState) {
            case ALIVE:
              alive();
              break;
            case COMPLETED:
              completed();
              break;
            case KILLED:
              killed();
              break;
            case ERROR:
              error(cause);
              break;
          }
        }

        @Override
        public void alive() {
          controllerFuture.complete(controller);
        }

        @Override
        public void completed() {
          controllerFuture.complete(controller);
          programCompletion.complete(ProgramController.State.COMPLETED);
        }

        @Override
        public void killed() {
          controllerFuture.complete(controller);
          programCompletion.complete(ProgramController.State.KILLED);
        }

        @Override
        public void error(Throwable cause) {
          controllerFuture.complete(controller);
          programCompletion.completeExceptionally(cause);
        }
      }, Threads.SAME_THREAD_EXECUTOR);

      // Block on the completion
      programCompletion.get();
    } catch (InterruptedException e) {
      LOG.warn("Program {} interrupted.", programRunId.getProgram(), e);
    } catch (ExecutionException e) {
      LOG.error("Program {} execution failed.", programRunId.getProgram(), e);
      throw Throwables.propagate(Throwables.getRootCause(e));
    } finally {
      LOG.info("Program run {} completed. Releasing resources.", programRunId);

      // Close the Program and the ProgramRunner
      Closeables.closeQuietly(program);
      if (programRunner instanceof Closeable) {
        Closeables.closeQuietly((Closeable) programRunner);
      }

      stopServices();
    }
  }

  @Override
  public void stop() {
    try {
      // If the program is not completed, get the controller and call stop
      CompletableFuture<ProgramController.State> programCompletion = this.programCompletion;

      // If there is no program completion future or it is already done, simply return as there is nothing to stop.
      if (programCompletion == null || programCompletion.isDone()) {
        return;
      }

      // Don't block forever to get the controller. The controller future might be empty if there is
      // systematic failure such that program runner is not reacting correctly
      ProgramController controller = controllerFuture.get(5, TimeUnit.SECONDS);

      LOG.info("Stopping runnable: {}.", programRunId.getProgram());

      // Give some time for the program to stop
      controller.stop().get(maxStopSeconds, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw Throwables.propagate(e);
    }
  }

  private static ArtifactId convert(NamespaceId namespaceId, co.cask.cdap.api.artifact.ArtifactId artifactId) {
    NamespaceId namespace = namespaceId;
    if (artifactId.getScope() == ArtifactScope.SYSTEM) {
      namespace = NamespaceId.SYSTEM;
    }
    return namespace.artifact(artifactId.getName(), artifactId.getVersion().getVersion());
  }

  private static <T> T readFile(File file, Class<T> type) {
    try (Reader reader = new BufferedReader(new FileReader(file))) {
      return GSON.fromJson(reader, type);
    } catch (Exception e) {
      throw new IllegalArgumentException(
        String.format("Unable to read %s file at %s", type.getSimpleName(), file.getAbsolutePath()));
    }
  }
}
