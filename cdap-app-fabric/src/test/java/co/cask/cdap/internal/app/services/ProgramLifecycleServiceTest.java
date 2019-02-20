/*
 * Copyright © 2018-2019 Cask Data, Inc.
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

package co.cask.cdap.internal.app.services;

import co.cask.cdap.AllProgramsApp;
import co.cask.cdap.api.artifact.ArtifactId;
import co.cask.cdap.api.artifact.ArtifactScope;
import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.internal.app.runtime.SystemArguments;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.internal.app.store.RunRecordMeta;
import co.cask.cdap.internal.profile.ProfileService;
import co.cask.cdap.internal.provision.MockProvisioner;
import co.cask.cdap.internal.provision.ProvisioningService;
import co.cask.cdap.master.spi.program.ProgramOptions;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProfileId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.profile.Profile;
import co.cask.cdap.proto.provisioner.ProvisionerInfo;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * ProgramLifecycleService tests.
 */
public class ProgramLifecycleServiceTest extends AppFabricTestBase {
  private static ProgramLifecycleService programLifecycleService;
  private static ProfileService profileService;
  private static ProvisioningService provisioningService;

  @BeforeClass
  public static void setup() {
    Injector injector = getInjector();
    programLifecycleService = injector.getInstance(ProgramLifecycleService.class);
    profileService = injector.getInstance(ProfileService.class);
    provisioningService = injector.getInstance(ProvisioningService.class);
    provisioningService.startAndWait();
  }

  @AfterClass
  public static void shutdown() {
    provisioningService.stopAndWait();
  }

  @Test
  public void testEmptyRunsIsStopped() {
    Assert.assertEquals(ProgramStatus.STOPPED, ProgramLifecycleService.getProgramStatus(Collections.emptyList()));
  }

  @Test
  public void testProgramStatusFromSingleRun() {
    RunRecordMeta record = RunRecordMeta.builder()
      .setProgramRunId(NamespaceId.DEFAULT.app("app").mr("mr").run(RunIds.generate()))
      .setStartTime(System.currentTimeMillis())
      .setArtifactId(new ArtifactId("r", new ArtifactVersion("1.0"), ArtifactScope.USER))
      .setStatus(ProgramRunStatus.PENDING)
      .setSourceId(new byte[] { 0 })
      .build();

    // pending or starting -> starting
    ProgramStatus status = ProgramLifecycleService.getProgramStatus(Collections.singleton(record));
    Assert.assertEquals(ProgramStatus.STARTING, status);

    record = RunRecordMeta.builder(record).setStatus(ProgramRunStatus.STARTING).build();
    status = ProgramLifecycleService.getProgramStatus(Collections.singleton(record));
    Assert.assertEquals(ProgramStatus.STARTING, status);

    // running, suspended, resuming -> running
    record = RunRecordMeta.builder(record).setStatus(ProgramRunStatus.RUNNING).build();
    status = ProgramLifecycleService.getProgramStatus(Collections.singleton(record));
    Assert.assertEquals(ProgramStatus.RUNNING, status);

    record = RunRecordMeta.builder(record).setStatus(ProgramRunStatus.SUSPENDED).build();
    status = ProgramLifecycleService.getProgramStatus(Collections.singleton(record));
    Assert.assertEquals(ProgramStatus.RUNNING, status);

    // failed, killed, completed -> stopped
    record = RunRecordMeta.builder(record).setStatus(ProgramRunStatus.FAILED).build();
    status = ProgramLifecycleService.getProgramStatus(Collections.singleton(record));
    Assert.assertEquals(ProgramStatus.STOPPED, status);

    record = RunRecordMeta.builder(record).setStatus(ProgramRunStatus.KILLED).build();
    status = ProgramLifecycleService.getProgramStatus(Collections.singleton(record));
    Assert.assertEquals(ProgramStatus.STOPPED, status);

    record = RunRecordMeta.builder(record).setStatus(ProgramRunStatus.COMPLETED).build();
    status = ProgramLifecycleService.getProgramStatus(Collections.singleton(record));
    Assert.assertEquals(ProgramStatus.STOPPED, status);
  }

  @Test
  public void testProgramStatusFromMultipleRuns() {
    ProgramId programId = NamespaceId.DEFAULT.app("app").mr("mr");
    RunRecordMeta pending = RunRecordMeta.builder()
      .setProgramRunId(programId.run(RunIds.generate()))
      .setStartTime(System.currentTimeMillis())
      .setArtifactId(new ArtifactId("r", new ArtifactVersion("1.0"), ArtifactScope.USER))
      .setStatus(ProgramRunStatus.PENDING)
      .setSourceId(new byte[] { 0 })
      .build();
    RunRecordMeta starting = RunRecordMeta.builder(pending)
      .setProgramRunId(programId.run(RunIds.generate()))
      .setStatus(ProgramRunStatus.STARTING).build();
    RunRecordMeta running = RunRecordMeta.builder(pending)
      .setProgramRunId(programId.run(RunIds.generate()))
      .setStatus(ProgramRunStatus.RUNNING).build();
    RunRecordMeta killed = RunRecordMeta.builder(pending)
      .setProgramRunId(programId.run(RunIds.generate()))
      .setStatus(ProgramRunStatus.KILLED).build();
    RunRecordMeta failed = RunRecordMeta.builder(pending)
      .setProgramRunId(programId.run(RunIds.generate()))
      .setStatus(ProgramRunStatus.FAILED).build();
    RunRecordMeta completed = RunRecordMeta.builder(pending)
      .setProgramRunId(programId.run(RunIds.generate()))
      .setStatus(ProgramRunStatus.COMPLETED).build();

    // running takes precedence over others
    ProgramStatus status = ProgramLifecycleService.getProgramStatus(
      Arrays.asList(pending, starting, running, killed, failed, completed));
    Assert.assertEquals(ProgramStatus.RUNNING, status);

    // starting takes precedence over stopped
    status = ProgramLifecycleService.getProgramStatus(Arrays.asList(pending, killed, failed, completed));
    Assert.assertEquals(ProgramStatus.STARTING, status);
    status = ProgramLifecycleService.getProgramStatus(Arrays.asList(starting, killed, failed, completed));
    Assert.assertEquals(ProgramStatus.STARTING, status);

    // end states are stopped
    status = ProgramLifecycleService.getProgramStatus(Arrays.asList(killed, failed, completed));
    Assert.assertEquals(ProgramStatus.STOPPED, status);
  }

  @Test
  public void testProfileProgramTypeRestrictions() throws Exception {
    deploy(AllProgramsApp.class, 200);
    ProfileId profileId = NamespaceId.DEFAULT.profile("profABC");
    ProvisionerInfo provisionerInfo = new ProvisionerInfo(MockProvisioner.NAME, Collections.emptyList());
    Profile profile = new Profile("profABC", "label", "desc", provisionerInfo);
    profileService.createIfNotExists(profileId, profile);

    try {
      Map<String, String> userArgs = new HashMap<>();
      userArgs.put(SystemArguments.PROFILE_NAME, profileId.getProfile());
      Map<String, String> systemArgs = new HashMap<>();

      Set<ProgramId> programIds = ImmutableSet.of(
        NamespaceId.DEFAULT.app(AllProgramsApp.NAME).program(ProgramType.SPARK, AllProgramsApp.NoOpSpark.NAME),
        NamespaceId.DEFAULT.app(AllProgramsApp.NAME).program(ProgramType.MAPREDUCE, AllProgramsApp.NoOpMR.NAME),
        NamespaceId.DEFAULT.app(AllProgramsApp.NAME).program(ProgramType.SERVICE, AllProgramsApp.NoOpService.NAME),
        NamespaceId.DEFAULT.app(AllProgramsApp.NAME).program(ProgramType.WORKER, AllProgramsApp.NoOpWorker.NAME)
      );

      Set<ProgramType> allowCustomProfiles = EnumSet.of(ProgramType.MAPREDUCE, ProgramType.SPARK, ProgramType.WORKFLOW);
      for (ProgramId programId : programIds) {
        ProgramOptions options = programLifecycleService.createProgramOptions(programId, userArgs,
                                                                              systemArgs, false);
        Optional<ProfileId> opt = SystemArguments.getProfileIdFromArgs(NamespaceId.DEFAULT,
                                                                       options.getArguments().asMap());
        Assert.assertTrue(opt.isPresent());
        if (allowCustomProfiles.contains(programId.getType())) {
          Assert.assertEquals(profileId, opt.get());
        } else {
          Assert.assertEquals(ProfileId.NATIVE, opt.get());
        }
      }
    } finally {
      profileService.disableProfile(profileId);
      profileService.deleteProfile(profileId);
    }
  }
}
