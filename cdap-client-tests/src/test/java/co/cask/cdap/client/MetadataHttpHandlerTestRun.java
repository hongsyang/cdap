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

package co.cask.cdap.client;

import co.cask.cdap.AppWithDataset;
import co.cask.cdap.StandaloneTester;
import co.cask.cdap.api.Config;
import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.artifact.ArtifactSummary;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.dataset.table.TableProperties;
import co.cask.cdap.api.metadata.Metadata;
import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.api.metadata.MetadataScope;
import co.cask.cdap.app.program.ManifestFields;
import co.cask.cdap.client.app.AllProgramsApp;
import co.cask.cdap.client.app.ConfigurableServiceApp;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.id.Id;
import co.cask.cdap.common.metadata.MetadataRecord;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.data2.metadata.dataset.SortInfo;
import co.cask.cdap.data2.metadata.system.AbstractSystemMetadataWriter;
import co.cask.cdap.data2.metadata.system.DatasetSystemMetadataProvider;
import co.cask.cdap.internal.app.deploy.Specifications;
import co.cask.cdap.metadata.MetadataHttpHandler;
import co.cask.cdap.proto.DatasetInstanceConfiguration;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.Ids;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.proto.id.ProfileId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.metadata.MetadataSearchResponse;
import co.cask.cdap.proto.metadata.MetadataSearchResultRecord;
import co.cask.cdap.spi.metadata.MetadataConstants;
import co.cask.common.http.HttpRequest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.jar.Manifest;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Tests for {@link MetadataHttpHandler}
 */
public class MetadataHttpHandlerTestRun extends MetadataTestBase {

  private final ApplicationId application = NamespaceId.DEFAULT.app(AppWithDataset.class.getSimpleName());
  private final ArtifactId artifactId = NamespaceId.DEFAULT.artifact(application.getApplication(), "1.0.0");
  private final ProgramId pingService = application.service("PingService");
  private final ProgramRunId runId = pingService.run(RunIds.generate());
  private final DatasetId myds = NamespaceId.DEFAULT.dataset("myds");
  private final MetadataEntity fieldEntity =
    MetadataEntity.builder(MetadataEntity.ofDataset(NamespaceId.DEFAULT.getNamespace(), "myds"))
      .appendAsType("field", "empname").build();

  @Before
  public void before() throws Exception {
    addAppArtifact(artifactId, AppWithDataset.class);
    AppRequest<Config> appRequest = new AppRequest<>(
      new ArtifactSummary(artifactId.getArtifact(), artifactId.getVersion()));
    appClient.deploy(application, appRequest);
    // Ensure the system metadata has been processed
    Tasks.waitFor(false, () -> getProperties(artifactId, MetadataScope.SYSTEM).isEmpty(), 10, TimeUnit.SECONDS);
    Tasks.waitFor(false, () -> getProperties(application, MetadataScope.SYSTEM).isEmpty(), 10, TimeUnit.SECONDS);
    Tasks.waitFor(false, () -> getProperties(pingService, MetadataScope.SYSTEM).isEmpty(), 10, TimeUnit.SECONDS);
  }

  @After
  public void after() throws Exception {
    appClient.delete(application);
    artifactClient.delete(artifactId);
    namespaceClient.delete(NamespaceId.DEFAULT);
    // Ensure all metadata has been removed.
    // Otherwise it is possible that a delete/drop is processed while the next test is running, failing that test.
    Tasks.waitFor(true, () -> getProperties(artifactId, MetadataScope.SYSTEM).isEmpty(), 10, TimeUnit.SECONDS);
    Tasks.waitFor(true, () -> getProperties(application, MetadataScope.SYSTEM).isEmpty(), 10, TimeUnit.SECONDS);
    Tasks.waitFor(true, () -> getProperties(pingService, MetadataScope.SYSTEM).isEmpty(), 10, TimeUnit.SECONDS);
  }

  @Test
  public void testProperties() throws Exception {
    // should fail because we haven't provided any metadata in the request
    addProperties(application, null, BadRequestException.class);
    String multiWordValue = "wow1 WoW2   -    WOW3 - wow4_woW5 wow6";
    Map<String, String> appProperties = ImmutableMap.of("aKey", "aValue", "multiword", multiWordValue);
    addProperties(application, appProperties);
    // should fail because we haven't provided any metadata in the request
    addProperties(pingService, null, BadRequestException.class);
    Map<String, String> serviceProperties = ImmutableMap.of("sKey", "sValue", "sK", "sV");
    addProperties(pingService, serviceProperties);
    // should fail because we haven't provided any metadata in the request
    addProperties(myds, null, BadRequestException.class);
    Map<String, String> datasetProperties = ImmutableMap.of("dKey", "dValue", "dK", "dV");
    addProperties(myds, datasetProperties);
    // should fail because we haven't provided any metadata in the request
    addProperties(artifactId, null, BadRequestException.class);
    Map<String, String> artifactProperties = ImmutableMap.of("rKey", "rValue", "rK", "rV");
    addProperties(artifactId, artifactProperties);
    // should fail because we haven't provided any metadata in the request
    addProperties(runId, null, BadRequestException.class);
    Map<String, String> runProperties = ImmutableMap.of("runKey", "runValue", "runK", "runV");
    addProperties(runId, runProperties);

    // retrieve properties and verify
    Map<String, String> properties = getProperties(application, MetadataScope.USER);
    Assert.assertEquals(appProperties, properties);
    properties = getProperties(pingService, MetadataScope.USER);
    Assert.assertEquals(serviceProperties, properties);
    properties = getProperties(myds, MetadataScope.USER);
    Assert.assertEquals(datasetProperties, properties);
    properties = getProperties(artifactId, MetadataScope.USER);
    Assert.assertEquals(artifactProperties, properties);
    properties = getProperties(runId, MetadataScope.USER);
    Assert.assertEquals(runProperties, properties);

    // test search for application
    Set<MetadataSearchResultRecord> searchProperties = searchMetadata(NamespaceId.DEFAULT, "aKey:aValue",
                                                                      MetadataEntity.APPLICATION);
    Set<MetadataSearchResultRecord> expected = ImmutableSet.of(new MetadataSearchResultRecord(application));
    Assert.assertEquals(expected, searchProperties);
    searchProperties = searchMetadata(NamespaceId.DEFAULT, "multiword:wow1", MetadataEntity.APPLICATION);
    Assert.assertEquals(expected, searchProperties);
    searchProperties = searchMetadata(NamespaceId.DEFAULT, "multiword:woW5", MetadataEntity.APPLICATION);
    Assert.assertEquals(expected, searchProperties);
    searchProperties = searchMetadata(NamespaceId.DEFAULT, "WOW3", MetadataEntity.APPLICATION);
    Assert.assertEquals(expected, searchProperties);

    // test search for artifact
    searchProperties = searchMetadata(NamespaceId.DEFAULT, "rKey:rValue", MetadataEntity.ARTIFACT);
    expected = ImmutableSet.of(new MetadataSearchResultRecord(artifactId));
    Assert.assertEquals(expected, searchProperties);

    searchProperties = searchMetadata(NamespaceId.DEFAULT, "multiword:w*");
    expected = ImmutableSet.of(new MetadataSearchResultRecord(application));
    Assert.assertEquals(expected, searchProperties);

    searchProperties = searchMetadata(NamespaceId.DEFAULT, "multiword:*");
    Assert.assertEquals(expected, searchProperties);

    searchProperties = searchMetadata(NamespaceId.DEFAULT, "wo*");
    Assert.assertEquals(expected, searchProperties);

    // test prefix search for service
    searchProperties = searchMetadata(NamespaceId.DEFAULT, "sKey:s*");
    expected = ImmutableSet.of(new MetadataSearchResultRecord(pingService));
    Assert.assertEquals(expected, searchProperties);

    // search without any target param
    searchProperties = searchMetadata(NamespaceId.DEFAULT, "sKey:s*");
    Assert.assertEquals(expected, searchProperties);

    // Should get empty
    searchProperties = searchMetadata(NamespaceId.DEFAULT, "sKey:s");
    Assert.assertEquals(0, searchProperties.size());

    searchProperties = searchMetadata(NamespaceId.DEFAULT, "s");
    Assert.assertEquals(0, searchProperties.size());

    // search non-existent property should return empty set
    searchProperties = searchMetadata(NamespaceId.DEFAULT, "NullKey:s*");
    Assert.assertEquals(ImmutableSet.<MetadataSearchResultRecord>of(), searchProperties);

    // search invalid ns should return empty set
    searchProperties = searchMetadata(new NamespaceId("invalidnamespace"), "sKey:s*");
    Assert.assertEquals(ImmutableSet.of(), searchProperties);

    // test removal
    removeProperties(application);
    Assert.assertTrue(getProperties(application, MetadataScope.USER).isEmpty());
    removeProperty(pingService, "sKey");
    removeProperty(pingService, "sK");
    Assert.assertTrue(getProperties(pingService, MetadataScope.USER).isEmpty());
    removeProperty(myds, "dKey");
    Assert.assertEquals(ImmutableMap.of("dK", "dV"), getProperties(myds, MetadataScope.USER));
    removeProperty(runId, "runK");
    removeProperty(runId, "runKey");
    Assert.assertTrue(getProperties(runId, MetadataScope.USER).isEmpty());

    // cleanup
    removeProperties(application);
    removeProperties(pingService);
    removeProperties(myds);
    removeProperties(artifactId);
    removeProperties(runId);
    Assert.assertTrue(getProperties(application, MetadataScope.USER).isEmpty());
    Assert.assertTrue(getProperties(pingService, MetadataScope.USER).isEmpty());
    Assert.assertTrue(getProperties(myds, MetadataScope.USER).isEmpty());
    Assert.assertTrue(getProperties(artifactId, MetadataScope.USER).isEmpty());
    Assert.assertTrue(getProperties(runId, MetadataScope.USER).isEmpty());
  }

  @Test
  public void testTags() throws Exception {
    // should fail because we haven't provided any metadata in the request
    addTags(myds, null, BadRequestException.class);
    Set<String> datasetTags = ImmutableSet.of("dTag", "dT");
    addTags(myds, datasetTags);
    addTags(application, null, BadRequestException.class);
    Set<String> appTags = ImmutableSet.of("aTag", "aT", "Wow-WOW1", "WOW_WOW2");
    addTags(application, appTags);
    // should fail because we haven't provided any metadata in the request
    addTags(pingService, null, BadRequestException.class);
    Set<String> serviceTags = ImmutableSet.of("sTag", "sT");
    addTags(pingService, serviceTags);
    addTags(runId, null, BadRequestException.class);
    Set<String> runTags = ImmutableSet.of("runTag", "runT");
    addTags(runId, runTags);
    Set<String> artifactTags = ImmutableSet.of("rTag", "rT");
    addTags(artifactId, artifactTags);
    Set<String> fieldTags = ImmutableSet.of("fTag", "fT");
    addTags(fieldEntity, fieldTags);

    // retrieve tags and verify
    Set<String> tags = getTags(application, MetadataScope.USER);
    Assert.assertTrue(tags.containsAll(appTags));
    Assert.assertTrue(appTags.containsAll(tags));
    tags = getTags(pingService, MetadataScope.USER);
    Assert.assertTrue(tags.containsAll(serviceTags));
    Assert.assertTrue(serviceTags.containsAll(tags));
    tags = getTags(runId, MetadataScope.USER);
    Assert.assertTrue(tags.containsAll(runTags));
    Assert.assertTrue(runTags.containsAll(tags));
    tags = getTags(myds, MetadataScope.USER);
    Assert.assertTrue(tags.containsAll(datasetTags));
    Assert.assertTrue(datasetTags.containsAll(tags));
    tags = getTags(artifactId, MetadataScope.USER);
    Assert.assertTrue(tags.containsAll(artifactTags));
    Assert.assertTrue(artifactTags.containsAll(tags));
    tags = getTags(fieldEntity, MetadataScope.USER);
    Assert.assertTrue(tags.containsAll(fieldTags));
    Assert.assertTrue(fieldTags.containsAll(tags));

    // test prefix search, should match dataset and application
    Set<MetadataSearchResultRecord> searchTags =
      searchMetadata(NamespaceId.DEFAULT, "Wow*");
    Set<MetadataSearchResultRecord> expected = ImmutableSet.of(new MetadataSearchResultRecord(application));
    Assert.assertEquals(expected, searchTags);

    // search without any target param
    searchTags = searchMetadata(NamespaceId.DEFAULT, "Wow*");
    Assert.assertEquals(expected, searchTags);

    // search non-existent tags should return empty set
    searchTags = searchMetadata(NamespaceId.DEFAULT, "NullKey");
    Assert.assertEquals(ImmutableSet.<MetadataSearchResultRecord>of(), searchTags);

    // test removal
    removeTag(application, "aTag");
    Assert.assertEquals(ImmutableSet.of("aT", "Wow-WOW1", "WOW_WOW2"), getTags(application, MetadataScope.USER));
    removeTags(pingService);
    Assert.assertTrue(getTags(pingService, MetadataScope.USER).isEmpty());
    removeTags(pingService);
    Assert.assertTrue(getTags(pingService, MetadataScope.USER).isEmpty());
    removeTags(runId);
    Assert.assertTrue(getTags(runId, MetadataScope.USER).isEmpty());
    removeTag(myds, "dT");
    Assert.assertEquals(ImmutableSet.of("dTag"), getTags(myds, MetadataScope.USER));
    removeTag(artifactId, "rTag");
    removeTag(artifactId, "rT");
    Assert.assertTrue(getTags(artifactId, MetadataScope.USER).isEmpty());
    // cleanup
    removeTags(application);
    removeTags(pingService);
    removeTags(runId);
    removeTags(myds);
    removeTags(artifactId);
    Assert.assertTrue(getTags(application, MetadataScope.USER).isEmpty());
    Assert.assertTrue(getTags(pingService, MetadataScope.USER).isEmpty());
    Assert.assertTrue(getTags(runId, MetadataScope.USER).isEmpty());
    Assert.assertTrue(getTags(myds, MetadataScope.USER).isEmpty());
    Assert.assertTrue(getTags(artifactId, MetadataScope.USER).isEmpty());
  }

  @Test
  public void testMetadata() throws Exception {
    assertCleanState();
    // Remove when nothing exists
    removeAllMetadata();
    assertCleanState();
    // Add some properties and tags
    Map<String, String> appProperties = ImmutableMap.of("aKey", "aValue");
    Map<String, String> serviceProperties = ImmutableMap.of("sKey", "sValue");
    Map<String, String> runProperties = ImmutableMap.of("runKey", "runValue");
    Map<String, String> datasetProperties = ImmutableMap.of("dKey", "dValue");
    Map<String, String> artifactProperties = ImmutableMap.of("rKey", "rValue");
    Map<String, String> fieldProperties = ImmutableMap.of("fKey", "fValue");
    Set<String> appTags = ImmutableSet.of("aTag");
    Set<String> serviceTags = ImmutableSet.of("sTag");
    Set<String> runTags = ImmutableSet.of("runTag");
    Set<String> datasetTags = ImmutableSet.of("dTag");
    Set<String> artifactTags = ImmutableSet.of("rTag");
    Set<String> fieldTags = ImmutableSet.of("fTag");
    addProperties(application, appProperties);
    addProperties(pingService, serviceProperties);
    addProperties(runId, runProperties);
    addProperties(myds, datasetProperties);
    addProperties(artifactId, artifactProperties);
    addProperties(fieldEntity, fieldProperties);
    addTags(application, appTags);
    addTags(pingService, serviceTags);
    addTags(runId, runTags);
    addTags(myds, datasetTags);
    addTags(artifactId, artifactTags);
    addTags(fieldEntity, fieldTags);
    // verify app
    Set<MetadataRecord> metadataRecords = getMetadata(application.toMetadataEntity(), MetadataScope.USER);
    Assert.assertEquals(1, metadataRecords.size());
    MetadataRecord metadata = metadataRecords.iterator().next();
    Assert.assertEquals(MetadataScope.USER, metadata.getScope());
    Assert.assertEquals(application.toMetadataEntity(), metadata.getMetadataEntity());
    Assert.assertEquals(appProperties, metadata.getProperties());
    Assert.assertEquals(appTags, metadata.getTags());
    // verify service
    metadataRecords = getMetadata(pingService.toMetadataEntity(), MetadataScope.USER);
    Assert.assertEquals(1, metadataRecords.size());
    metadata = metadataRecords.iterator().next();
    Assert.assertEquals(MetadataScope.USER, metadata.getScope());
    Assert.assertEquals(pingService.toMetadataEntity(), metadata.getMetadataEntity());
    Assert.assertEquals(serviceProperties, metadata.getProperties());
    Assert.assertEquals(serviceTags, metadata.getTags());
    // We specifically call the getMetadata where there is no aggregation for runId metadata.
    // For aggregated version of test see LineageHttpHandlerTest which calls getMetadataForRun().
    Set<MetadataRecord> runMetadataRecords = getMetadata(runId.toMetadataEntity(), MetadataScope.USER);
    Assert.assertEquals(1, runMetadataRecords.size());
    MetadataRecord runMetadata = runMetadataRecords.iterator().next();
    Assert.assertEquals(MetadataScope.USER, runMetadata.getScope());
    Assert.assertEquals(runId.toMetadataEntity(), runMetadata.getMetadataEntity());
    Assert.assertEquals(runProperties, runMetadata.getProperties());
    Assert.assertEquals(runTags, runMetadata.getTags());
    // verify dataset
    metadataRecords = getMetadata(myds.toMetadataEntity(), MetadataScope.USER);
    Assert.assertEquals(1, metadataRecords.size());
    metadata = metadataRecords.iterator().next();
    Assert.assertEquals(MetadataScope.USER, metadata.getScope());
    Assert.assertEquals(myds.toMetadataEntity(), metadata.getMetadataEntity());
    Assert.assertEquals(datasetProperties, metadata.getProperties());
    Assert.assertEquals(datasetTags, metadata.getTags());
    // verify artifact
    metadataRecords = getMetadata(artifactId.toMetadataEntity(), MetadataScope.USER);
    Assert.assertEquals(1, metadataRecords.size());
    metadata = metadataRecords.iterator().next();
    Assert.assertEquals(MetadataScope.USER, metadata.getScope());
    Assert.assertEquals(artifactId.toMetadataEntity(), metadata.getMetadataEntity());
    Assert.assertEquals(artifactProperties, metadata.getProperties());
    Assert.assertEquals(artifactTags, metadata.getTags());
    // verify custom entity
    metadataRecords = getMetadata(fieldEntity, MetadataScope.USER);
    Assert.assertEquals(1, metadataRecords.size());
    MetadataRecord metadataRecord = metadataRecords.iterator().next();
    Assert.assertEquals(MetadataScope.USER, metadataRecord.getScope());
    Assert.assertEquals(fieldEntity, metadataRecord.getMetadataEntity());
    Assert.assertEquals(fieldProperties, metadataRecord.getProperties());
    Assert.assertEquals(fieldTags, metadataRecord.getTags());
    // cleanup
    removeAllMetadata();
    assertCleanState();
  }

  @Test
  public void testDeleteApplication() throws Exception {
    namespaceClient.create(new NamespaceMeta.Builder().setName(TEST_NAMESPACE1).build());
    appClient.deploy(TEST_NAMESPACE1, createAppJarFile(AllProgramsApp.class));
    ProgramId programId = TEST_NAMESPACE1.app(AllProgramsApp.NAME).service(AllProgramsApp.NoOpService.NAME);

    // Set some properties metadata
    Map<String, String> flowProperties = ImmutableMap.of("sKey", "sValue", "sK", "sV");
    addProperties(programId, flowProperties);

    // Get properties
    Map<String, String> properties = getProperties(programId, MetadataScope.USER);
    Assert.assertEquals(2, properties.size());

    // Delete the App after stopping the flow
    appClient.delete(TEST_NAMESPACE1.app(programId.getApplication()));

    // Delete again should throw not found exception
    try {
      appClient.delete(TEST_NAMESPACE1.app(programId.getApplication()));
      Assert.fail("Expected NotFoundException");
    } catch (NotFoundException e) {
      // expected
    }

    // Now try to get from invalid entity should throw 404.
    Tasks.waitFor(new HashMap<>(), () -> getProperties(programId, MetadataScope.USER), 10, TimeUnit.SECONDS);
  }

  @Test
  public void testInvalidProperties() {
    // Test length
    StringBuilder builder = new StringBuilder(100);
    for (int i = 0; i < 100; i++) {
      builder.append("a");
    }
    Map<String, String> properties = ImmutableMap.of("aKey", builder.toString());
    addProperties(application, properties, BadRequestException.class);
    properties = ImmutableMap.of(builder.toString(), "aValue");
    addProperties(application, properties, BadRequestException.class);

    // Try to add tag as property
    properties = ImmutableMap.of("tags", "aValue");
    addProperties(application, properties, BadRequestException.class);

    // Invalid chars
    properties = ImmutableMap.of("aKey$", "aValue");
    addProperties(application, properties, BadRequestException.class);

    properties = ImmutableMap.of("aKey", "aValue$");
    addProperties(application, properties, BadRequestException.class);
  }

  @Test
  public void testInvalidTags() {
    // Invalid chars
    Set<String> tags = ImmutableSet.of("aTag$");
    addTags(application, tags, BadRequestException.class);

    // Test length
    StringBuilder builder = new StringBuilder(100);
    for (int i = 0; i < 100; i++) {
      builder.append("a");
    }
    tags = ImmutableSet.of(builder.toString());
    addTags(application, tags, BadRequestException.class);
  }

  @Test
  public void testDeletedProgramHandlerStage() throws Exception {
    // Deploy an app with 2 services
    appClient.deploy(TEST_NAMESPACE1, createAppJarFile(ConfigurableServiceApp.class),
                     new ConfigurableServiceApp.ServiceConfig(2));

    ProgramId program = TEST_NAMESPACE1.app(ConfigurableServiceApp.NAME)
      .service(ConfigurableServiceApp.SERVICE_NAME_BASE + 1);
    Tasks.waitFor(false, () -> getProperties(program, MetadataScope.SYSTEM).isEmpty(), 10, TimeUnit.SECONDS);

    // Set some properties metadata
    Map<String, String> flowProperties = ImmutableMap.of("sKey", "sValue", "sK", "sV");
    addProperties(program, flowProperties);

    // Get properties
    Map<String, String> properties = getProperties(program, MetadataScope.USER);
    Assert.assertEquals(2, properties.size());

    // Deploy the app again, but with one less service.
    appClient.deploy(TEST_NAMESPACE1, createAppJarFile(ConfigurableServiceApp.class),
                     new ConfigurableServiceApp.ServiceConfig(1));

    // Get properties from deleted service - should return 404
    Tasks.waitFor(true, () -> getProperties(program, MetadataScope.USER).isEmpty(), 10, TimeUnit.SECONDS);

    // Delete the App after stopping the flow
    appClient.delete(program.getParent());
  }

  @Test
  public void testSystemMetadataRetrieval() throws Exception {
    appClient.deploy(NamespaceId.DEFAULT, createAppJarFile(AllProgramsApp.class));

    // verify dataset system metadata
    DatasetId datasetInstance = NamespaceId.DEFAULT.dataset(AllProgramsApp.DATASET_NAME);
    Tasks.waitFor(ImmutableSet.of(DatasetSystemMetadataProvider.BATCH_TAG, AbstractSystemMetadataWriter.EXPLORE_TAG),
                  () -> getTags(datasetInstance, MetadataScope.SYSTEM),
                  10, TimeUnit.SECONDS);

    Map<String, String> dsSystemProperties = getProperties(datasetInstance, MetadataScope.SYSTEM);
    // Verify create time exists, and is within the past hour
    Assert.assertTrue("Expected creation time to exist but it does not",
                      dsSystemProperties.containsKey(MetadataConstants.CREATION_TIME_KEY));
    long createTime = Long.parseLong(dsSystemProperties.get(MetadataConstants.CREATION_TIME_KEY));
    Assert.assertTrue("Dataset create time should be within the last hour - " + createTime,
                      createTime > System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1));

    // Now remove create time and assert all other system properties
    Assert.assertEquals(
      ImmutableMap.of(
        "type", KeyValueTable.class.getName(),
        MetadataConstants.DESCRIPTION_KEY, "test dataset",
        MetadataConstants.CREATION_TIME_KEY, String.valueOf(createTime),
        MetadataConstants.ENTITY_NAME_KEY, datasetInstance.getEntityName()
      ),
      dsSystemProperties
    );

    //Update properties, and make sure that system metadata gets updated (except create time)
    datasetClient.update(datasetInstance, TableProperties.builder().setTTL(100000L).build().getProperties());
    Tasks.waitFor(ImmutableMap.of("type", KeyValueTable.class.getName(),
                                  MetadataConstants.DESCRIPTION_KEY, "test dataset",
                                  MetadataConstants.TTL_KEY, "100000",
                                  MetadataConstants.CREATION_TIME_KEY, String.valueOf(createTime),
                                  MetadataConstants.ENTITY_NAME_KEY, datasetInstance.getEntityName()),
                  () -> getProperties(datasetInstance, MetadataScope.SYSTEM),
                  10, TimeUnit.SECONDS);

    // verify artifact metadata
    ArtifactId artifactId = getArtifactId();
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataRecord(artifactId.toMetadataEntity(), MetadataScope.SYSTEM,
                           ImmutableMap.of(MetadataConstants.ENTITY_NAME_KEY, artifactId.getEntityName()),
                           ImmutableSet.of())
      ),
      removeCreationTime(getMetadata(artifactId.toMetadataEntity(), MetadataScope.SYSTEM))
    );
    // verify app system metadata
    ApplicationId app = NamespaceId.DEFAULT.app(AllProgramsApp.NAME);
    Assert.assertEquals(
      ImmutableMap.builder()
        .put(ProgramType.MAPREDUCE.getPrettyName() + MetadataConstants.KEYVALUE_SEPARATOR + AllProgramsApp.NoOpMR.NAME,
             AllProgramsApp.NoOpMR.NAME)
        .put(ProgramType.MAPREDUCE.getPrettyName() + MetadataConstants.KEYVALUE_SEPARATOR + AllProgramsApp.NoOpMR2.NAME,
             AllProgramsApp.NoOpMR2.NAME)
        .put(ProgramType.SERVICE.getPrettyName() + MetadataConstants.KEYVALUE_SEPARATOR +
               AllProgramsApp.NoOpService.NAME, AllProgramsApp.NoOpService.NAME)
        .put(ProgramType.SPARK.getPrettyName() + MetadataConstants.KEYVALUE_SEPARATOR + AllProgramsApp.NoOpSpark.NAME,
             AllProgramsApp.NoOpSpark.NAME)
        .put(ProgramType.WORKER.getPrettyName() + MetadataConstants.KEYVALUE_SEPARATOR + AllProgramsApp.NoOpWorker.NAME,
             AllProgramsApp.NoOpWorker.NAME)
        .put(ProgramType.WORKFLOW.getPrettyName() + MetadataConstants.KEYVALUE_SEPARATOR
               + AllProgramsApp.NoOpWorkflow.NAME, AllProgramsApp.NoOpWorkflow.NAME)
        .put(MetadataConstants.ENTITY_NAME_KEY, app.getEntityName())
        .put(AbstractSystemMetadataWriter.VERSION_KEY, ApplicationId.DEFAULT_VERSION)
        .put(MetadataConstants.DESCRIPTION_KEY, AllProgramsApp.DESCRIPTION)
        .put("schedule" + MetadataConstants.KEYVALUE_SEPARATOR + AllProgramsApp.SCHEDULE_NAME,
             AllProgramsApp.SCHEDULE_NAME + MetadataConstants.KEYVALUE_SEPARATOR + AllProgramsApp.SCHEDULE_DESCRIPTION)
        .build(),
      removeCreationTime(getProperties(app, MetadataScope.SYSTEM)));
    Assert.assertEquals(ImmutableSet.of(AllProgramsApp.class.getSimpleName()),
                        getTags(app, MetadataScope.SYSTEM));
    // verify program system metadata, we now have profile as system metadata for workflow
    assertProgramSystemMetadata(app.worker(AllProgramsApp.NoOpWorker.NAME), "Realtime", null, null);
    assertProgramSystemMetadata(app.service(AllProgramsApp.NoOpService.NAME), "Realtime", null, null);
    assertProgramSystemMetadata(app.mr(AllProgramsApp.NoOpMR.NAME), "Batch", null, ProfileId.NATIVE);
    assertProgramSystemMetadata(app.spark(AllProgramsApp.NoOpSpark.NAME), "Batch", null, ProfileId.NATIVE);
    assertProgramSystemMetadata(app.workflow(AllProgramsApp.NoOpWorkflow.NAME), "Batch",
                                AllProgramsApp.NoOpWorkflow.DESCRIPTION, ProfileId.NATIVE);

    // update dataset properties to add the workflow.local.dataset property to it.
    try {
      datasetClient.update(datasetInstance,
                           Collections.singletonMap(Constants.AppFabric.WORKFLOW_LOCAL_DATASET_PROPERTY,
                                                    Boolean.TRUE.toString()));

      Tasks.waitFor(ImmutableSet.of(DatasetSystemMetadataProvider.BATCH_TAG,
                                    AbstractSystemMetadataWriter.EXPLORE_TAG,
                                    DatasetSystemMetadataProvider.LOCAL_DATASET_TAG),
                    () -> getTags(datasetInstance, MetadataScope.SYSTEM),
                    10, TimeUnit.SECONDS);
    } finally {
      // Always reset the property. Otherwise later test could fail due to bug in CDAP-8220
      datasetClient.update(datasetInstance,
                           Collections.singletonMap(Constants.AppFabric.WORKFLOW_LOCAL_DATASET_PROPERTY,
                                                    Boolean.FALSE.toString()));
    }
  }

  @Test
  public void testExploreSystemTags() throws Exception {
    appClient.deploy(NamespaceId.DEFAULT, createAppJarFile(AllProgramsApp.class));

    //verify dataset is explorable
    // verify fileSet is explorable
    DatasetId datasetInstance = NamespaceId.DEFAULT.dataset(AllProgramsApp.DATASET_NAME4);
    Tasks.waitFor(ImmutableSet.of(DatasetSystemMetadataProvider.BATCH_TAG,
                                  AbstractSystemMetadataWriter.EXPLORE_TAG),
                  () -> getTags(datasetInstance, MetadataScope.SYSTEM),
                  10, TimeUnit.SECONDS);

    //verify partitionedFileSet is explorable
    DatasetId datasetInstance2 = NamespaceId.DEFAULT.dataset(AllProgramsApp.DATASET_NAME5);
    Set<String> dsSystemTags2 = getTags(datasetInstance2, MetadataScope.SYSTEM);
    Assert.assertEquals(
      ImmutableSet.of(DatasetSystemMetadataProvider.BATCH_TAG,
                      AbstractSystemMetadataWriter.EXPLORE_TAG),
      dsSystemTags2);

    //verify that fileSet that isn't set to explorable does not have explore tag
    DatasetId datasetInstance3 = NamespaceId.DEFAULT.dataset(AllProgramsApp.DATASET_NAME6);
    Set<String> dsSystemTags3 = getTags(datasetInstance3, MetadataScope.SYSTEM);
    Assert.assertFalse(dsSystemTags3.contains(AbstractSystemMetadataWriter.EXPLORE_TAG));
    Assert.assertTrue(dsSystemTags3.contains(DatasetSystemMetadataProvider.BATCH_TAG));

    //verify that partitioned fileSet that isn't set to explorable does not have explore tag
    DatasetId datasetInstance4 = NamespaceId.DEFAULT.dataset(AllProgramsApp.DATASET_NAME7);
    Set<String> dsSystemTags4 = getTags(datasetInstance4, MetadataScope.SYSTEM);
    Assert.assertFalse(dsSystemTags4.contains(AbstractSystemMetadataWriter.EXPLORE_TAG));
    Assert.assertTrue(dsSystemTags4.contains(DatasetSystemMetadataProvider.BATCH_TAG));
  }

  @Test
  public void testSearchUsingSystemMetadata() throws Exception {
    appClient.deploy(NamespaceId.DEFAULT, createAppJarFile(AllProgramsApp.class));
    ApplicationId app = NamespaceId.DEFAULT.app(AllProgramsApp.NAME);
    ArtifactId artifact = getArtifactId();
    try {
      // search artifacts
      assertArtifactSearch();
      // search app
      assertAppSearch(app, artifact);
      // search programs
      assertProgramSearch(app);
      // search data entities
      assertDataEntitySearch();
    } finally {
      // cleanup
      appClient.delete(app);
      artifactClient.delete(artifact);
    }
  }

  @Test
  public void testSystemScopeArtifacts() throws Exception {
    // add a system artifact. currently can't do this through the rest api (by design)
    // so bypass it and use the repository directly
    ArtifactId systemId = NamespaceId.SYSTEM.artifact("app", "1.0.0");
    File systemArtifact = createArtifactJarFile(AllProgramsApp.class, "app", "1.0.0", new Manifest());

    StandaloneTester tester = STANDALONE.get();
    tester.addSystemArtifact(systemId.getArtifact(),
                             Id.Artifact.fromEntityId(systemId).getVersion(), systemArtifact, null);
    // wait until the system metadata has been processed
    Tasks.waitFor(false, () -> getProperties(systemId, MetadataScope.SYSTEM).isEmpty(), 10, TimeUnit.SECONDS);

    // verify that user metadata can be added for system-scope artifacts
    Map<String, String> userProperties = ImmutableMap.of("systemArtifactKey", "systemArtifactValue");
    Set<String> userTags = ImmutableSet.of();
    addProperties(systemId, userProperties);
    addTags(systemId, userTags);

    // verify that user and system metadata can be retrieved for system-scope artifacts
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataRecord(systemId, MetadataScope.USER, userProperties, userTags),
        new MetadataRecord(systemId, MetadataScope.SYSTEM,
                           ImmutableMap.of(MetadataConstants.ENTITY_NAME_KEY, systemId.getEntityName()),
                           ImmutableSet.of())
      ),
      removeCreationTime(getMetadata(systemId.toMetadataEntity()))
    );

    // verify that system scope artifacts can be returned by a search in the default namespace
    // with no target type
    Assert.assertEquals(
      ImmutableSet.of(new MetadataSearchResultRecord(systemId)),
      searchMetadata(NamespaceId.DEFAULT, "system*")
    );
    // with target type as artifact
    Assert.assertEquals(
      ImmutableSet.of(new MetadataSearchResultRecord(systemId)),
      searchMetadata(NamespaceId.DEFAULT, "system*", MetadataEntity.ARTIFACT)
    );

    // verify that user metadata can be deleted for system-scope artifacts
    removeMetadata(systemId);
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataRecord(systemId, MetadataScope.USER, ImmutableMap.of(), ImmutableSet.of()),
        new MetadataRecord(systemId, MetadataScope.SYSTEM,
                           ImmutableMap.of(MetadataConstants.ENTITY_NAME_KEY, systemId.getEntityName()),
                           ImmutableSet.of())
      ),
      removeCreationTime(getMetadata(systemId.toMetadataEntity()))
    );
    artifactClient.delete(systemId);
  }

  @Test
  public void testScopeQueryParam() throws Exception {
    appClient.deploy(NamespaceId.DEFAULT, createAppJarFile(AllProgramsApp.class));
    ApplicationId app = NamespaceId.DEFAULT.app(AllProgramsApp.NAME);
    RESTClient restClient = new RESTClient(clientConfig);
    URL url = clientConfig.resolveNamespacedURLV3(NamespaceId.DEFAULT,
                                                  "apps/" + AllProgramsApp.NAME + "/metadata?scope=system");
    Assert.assertEquals(
      HttpResponseStatus.OK.code(),
      restClient.execute(HttpRequest.get(url).build()).getResponseCode()
    );
    url = clientConfig.resolveNamespacedURLV3(NamespaceId.DEFAULT,
                                              "datasets/" + AllProgramsApp.DATASET_NAME +
                                                "/metadata/properties?scope=SySTeM");
    Assert.assertEquals(
      HttpResponseStatus.OK.code(),
      restClient.execute(HttpRequest.get(url).build()).getResponseCode()
    );
    url = clientConfig.resolveNamespacedURLV3(NamespaceId.DEFAULT,
                                              "apps/" + AllProgramsApp.NAME + "/services/" +
                                                AllProgramsApp.NoOpService.NAME + "/metadata/tags?scope=USER");
    Assert.assertEquals(
      HttpResponseStatus.OK.code(),
      restClient.execute(HttpRequest.get(url).build()).getResponseCode()
    );
    appClient.delete(app);

    // deleting the app does not delete the dataset, delete them explicitly to clear their system metadata
    ApplicationSpecification spec = Specifications.from(new AllProgramsApp());
    for (String dataset : spec.getDatasets().keySet()) {
      datasetClient.delete(NamespaceId.DEFAULT.dataset(dataset));
    }
  }

  @Test
  public void testSearchTargetType() throws Exception {
    NamespaceId namespace = Ids.namespace("testSearchTargetType");
    namespaceClient.create(new NamespaceMeta.Builder().setName(namespace).build());

    appClient.deploy(namespace, createAppJarFile(AllProgramsApp.class));

    // Add metadata to app
    Set<String> tags = ImmutableSet.of("utag1", "utag2");
    ApplicationId appId = namespace.app(AllProgramsApp.NAME);
    addTags(appId, tags);

    // Add metadata to dataset
    tags = ImmutableSet.of("utag21");
    DatasetId datasetId = namespace.dataset(AllProgramsApp.DATASET_NAME);
    addTags(datasetId, tags);

    // Search for single target type
    Assert.assertEquals(ImmutableSet.of(new MetadataSearchResultRecord(appId)),
                        searchMetadata(namespace, "utag*", MetadataEntity.APPLICATION));
    Assert.assertEquals(ImmutableSet.of(new MetadataSearchResultRecord(datasetId)),
                        searchMetadata(namespace, "utag*", MetadataEntity.DATASET));

    // Search for multiple target types
    Assert.assertEquals(ImmutableSet.of(new MetadataSearchResultRecord(datasetId),
                                        new MetadataSearchResultRecord(appId)),
                        searchMetadata(namespace, "utag*",
                                       ImmutableSet.of(
                                         MetadataEntity.APPLICATION,
                                         MetadataEntity.DATASET
                                       )));

    // Search for all target types
    Assert.assertEquals(ImmutableSet.of(new MetadataSearchResultRecord(datasetId),
                                        new MetadataSearchResultRecord(appId)),
                        searchMetadata(namespace, "utag*"));
    Assert.assertEquals(ImmutableSet.of(new MetadataSearchResultRecord(datasetId),
                                        new MetadataSearchResultRecord(appId)),
                        searchMetadata(namespace, "utag*"));
  }

  @Test
  public void testSearchMetadata() throws Exception {
    appClient.deploy(NamespaceId.DEFAULT, createAppJarFile(AllProgramsApp.class));

    // wait for the system metadata to be processed
    ApplicationId appId = NamespaceId.DEFAULT.app(AllProgramsApp.NAME);
    DatasetId datasetId = NamespaceId.DEFAULT.dataset(AllProgramsApp.DATASET_NAME);
    Tasks.waitFor(false, () -> getProperties(appId, MetadataScope.SYSTEM).isEmpty(), 10, TimeUnit.SECONDS);
    Tasks.waitFor(false, () -> getProperties(datasetId, MetadataScope.SYSTEM).isEmpty(), 10, TimeUnit.SECONDS);

    Map<NamespacedEntityId, Metadata> expectedUserMetadata = new HashMap<>();

    // Add metadata to app
    Map<String, String> props = ImmutableMap.of("key1", "value1");
    Set<String> tags = ImmutableSet.of("tag1", "tag2");
    addProperties(appId, props);
    addTags(appId, tags);
    expectedUserMetadata.put(appId, new Metadata(props, tags));

    // Add metadata to dataset
    props = ImmutableMap.of("key10", "value10", "key11", "value11");
    tags = ImmutableSet.of("tag11");
    addProperties(datasetId, props);
    addTags(datasetId, tags);
    expectedUserMetadata.put(datasetId, new Metadata(props, tags));

    Set<MetadataSearchResultRecord> results = super.searchMetadata(NamespaceId.DEFAULT, "value*", ImmutableSet.of());

    // Verify results
    Assert.assertEquals(expectedUserMetadata.keySet(), getEntities(results));
    for (MetadataSearchResultRecord result : results) {
      // User metadata has to match exactly since we know what we have set
      Assert.assertEquals(expectedUserMetadata.get(result.getEntityId()), result.getMetadata().get(MetadataScope.USER));
      // Make sure system metadata is returned, we cannot check for exact match since we haven't set it
      Metadata systemMetadata = result.getMetadata().get(MetadataScope.SYSTEM);
      Assert.assertNotNull(systemMetadata);
      Assert.assertFalse(systemMetadata.getProperties().isEmpty());
      Assert.assertFalse(systemMetadata.getTags().isEmpty());
    }

    // add metadata to field (custom entity)
    props = ImmutableMap.of("fKey1", "fValue1", "fKey2", "fValue2");
    tags = ImmutableSet.of("fTag1");
    MetadataEntity metadataEntity =
      MetadataEntity.builder(datasetId.toMetadataEntity()).appendAsType("field", "someField").build();
    addProperties(metadataEntity, props);
    addTags(metadataEntity, tags);
    Map<MetadataEntity, Metadata> expectedUserMetadataV2 = new HashMap<>();
    expectedUserMetadataV2.put(metadataEntity, new Metadata(props, tags));

    Set<MetadataSearchResultRecord> resultsV2 = super.searchMetadata(NamespaceId.DEFAULT, "fValue*",
                                                                     ImmutableSet.of(), null, 0, Integer.MAX_VALUE,
                                                                     0, null, false).getResults();

    // Verify results
    Assert.assertEquals(expectedUserMetadataV2.keySet(), getMetadataEntities(resultsV2));
    for (MetadataSearchResultRecord result : resultsV2) {
      // User metadata has to match exactly since we know what we have set
      Assert.assertEquals(expectedUserMetadataV2.get(result.getMetadataEntity()),
                          result.getMetadata().get(MetadataScope.USER));
      // Make sure system metadata is returned, we cannot check for exact match since we haven't set it
      Metadata systemMetadata = result.getMetadata().get(MetadataScope.SYSTEM);
      // custom entity should not have any system metadata for it
      Assert.assertNull(systemMetadata);
    }
  }

  @Test
  public void testCrossNamespaceSearchMetadata() throws Exception {
    NamespaceId namespace1 = new NamespaceId("ns1");
    namespaceClient.create(new NamespaceMeta.Builder().setName(namespace1).build());
    NamespaceId namespace2 = new NamespaceId("ns2");
    namespaceClient.create(new NamespaceMeta.Builder().setName(namespace2).build());

    try {
      appClient.deploy(namespace1, createAppJarFile(AllProgramsApp.class));
      appClient.deploy(namespace2, createAppJarFile(AllProgramsApp.class));

      // Add metadata to app
      Map<String, String> props = ImmutableMap.of("key1", "value1");
      Metadata meta = new Metadata(props, Collections.emptySet());
      ApplicationId app1Id = namespace1.app(AllProgramsApp.NAME);
      addProperties(app1Id, props);

      ApplicationId app2Id = namespace2.app(AllProgramsApp.NAME);
      addProperties(app2Id, props);

      MetadataSearchResponse results = super.searchMetadata(null, "value*", Collections.emptySet(),
                                                            null, 0, 10, 0, null, false);

      Map<MetadataEntity, Metadata> expected = new HashMap<>();
      expected.put(app1Id.toMetadataEntity(), meta);
      expected.put(app2Id.toMetadataEntity(), meta);

      Map<MetadataEntity, Metadata> actual = new HashMap<>();
      for (MetadataSearchResultRecord record : results.getResults()) {
        actual.put(record.getMetadataEntity(), record.getMetadata().get(MetadataScope.USER));
      }
      Assert.assertEquals(expected, actual);
    } finally {
      namespaceClient.delete(namespace1);
      namespaceClient.delete(namespace2);
    }
  }

  @Test
  public void testSearchMetadataDelete() throws Exception {
    NamespaceId namespace = new NamespaceId("ns1");
    namespaceClient.create(new NamespaceMeta.Builder().setName(namespace).build());

    // Deploy app
    appClient.deploy(namespace, createAppJarFile(AllProgramsApp.class, AllProgramsApp.class.getSimpleName(), "1.0"));

    ArtifactId artifact = namespace.artifact(AllProgramsApp.class.getSimpleName(), "1.0");
    ApplicationId app = namespace.app(AllProgramsApp.NAME);
    ProgramId service = app.service(AllProgramsApp.NoOpService.NAME);
    DatasetId datasetInstance = namespace.dataset(AllProgramsApp.DATASET_NAME);

    // wait for metadata to be processed
    Tasks.waitFor(false, () -> getProperties(app, MetadataScope.SYSTEM).isEmpty(), 10, TimeUnit.SECONDS);
    Tasks.waitFor(false, () -> getProperties(service, MetadataScope.SYSTEM).isEmpty(), 10, TimeUnit.SECONDS);
    Tasks.waitFor(false, () -> getProperties(datasetInstance, MetadataScope.SYSTEM).isEmpty(), 10, TimeUnit.SECONDS);

    Set<String> tags = ImmutableSet.of("tag1", "tag2");

    // Add metadata
    addTags(app, tags);
    addTags(datasetInstance, tags);

    // Assert metadata
    Assert.assertEquals(ImmutableSet.of(new MetadataSearchResultRecord(datasetInstance)),
                        searchMetadata(namespace, AllProgramsApp.DATASET_NAME));
    Assert.assertEquals(ImmutableSet.of(
                          new MetadataSearchResultRecord(app),
                          new MetadataSearchResultRecord(artifact)
                        ),
                        searchMetadata(namespace, "all*"));
    Assert.assertEquals(ImmutableSet.of(
                          new MetadataSearchResultRecord(app),
                          new MetadataSearchResultRecord(datasetInstance)
                        ),
                        searchMetadata(namespace, "tag1"));

    // Delete entities
    appClient.delete(app);
    datasetClient.delete(datasetInstance);
    artifactClient.delete(artifact);

    // Assert no metadata
    Tasks.waitFor(0, () -> searchMetadata(namespace, AllProgramsApp.DATASET_NAME).size(), 10, TimeUnit.SECONDS);
    Tasks.waitFor(0, () -> searchMetadata(namespace, "all*").size(), 10, TimeUnit.SECONDS);
    Tasks.waitFor(0, () -> searchMetadata(namespace, "tag1").size(), 10, TimeUnit.SECONDS);
  }

  @Test
  public void testSearchMetadataDeleteNamespace() throws Exception {
    NamespaceId namespace = new NamespaceId("ns2");
    namespaceClient.create(new NamespaceMeta.Builder().setName(namespace).build());

    // Deploy app
    appClient.deploy(namespace, createAppJarFile(AllProgramsApp.class, AllProgramsApp.class.getSimpleName(), "1.0"));

    Set<String> tags = ImmutableSet.of("tag1", "tag2");
    ArtifactId artifact = namespace.artifact(AllProgramsApp.class.getSimpleName(), "1.0");
    ApplicationId app = namespace.app(AllProgramsApp.NAME);
    DatasetId datasetInstance = namespace.dataset(AllProgramsApp.DATASET_NAME);

    // Add metadata
    addTags(app, tags);
    addTags(datasetInstance, tags);

    Tasks.waitFor(ImmutableSet.of(new MetadataSearchResultRecord(datasetInstance)),
                  () -> searchMetadata(namespace, AllProgramsApp.DATASET_NAME),
                  10, TimeUnit.SECONDS);
    Tasks.waitFor(ImmutableSet.of(new MetadataSearchResultRecord(app),
                                  new MetadataSearchResultRecord(artifact)),
                  () -> searchMetadata(namespace, "all*"),
                  10, TimeUnit.SECONDS);
    Assert.assertEquals(ImmutableSet.of(
                          new MetadataSearchResultRecord(app),
                          new MetadataSearchResultRecord(datasetInstance)
                        ),
                        searchMetadata(namespace, "tag1"));

    // Delete namespace
    namespaceClient.delete(namespace);

    // Assert no metadata
    Tasks.waitFor(0, () -> searchMetadata(namespace, AllProgramsApp.DATASET_NAME).size(), 10, TimeUnit.SECONDS);
    Tasks.waitFor(ImmutableSet.of(), () -> searchMetadata(namespace, "all*"), 10, TimeUnit.SECONDS);
    Tasks.waitFor(ImmutableSet.of(), () -> searchMetadata(namespace, "tag1"), 10, TimeUnit.SECONDS);
  }

  @Test
  public void testInvalidSearchParams() throws Exception {
    NamespaceId namespace = new NamespaceId("invalid");
    Set<String> targets = Collections.emptySet();
    try {
      searchMetadata(namespace, "*", targets, MetadataConstants.ENTITY_NAME_KEY);
      Assert.fail();
    } catch (BadRequestException e) {
      // expected
    }

    // search with bad sort field
    try {
      searchMetadata(namespace, "*", targets, "name asc");
      Assert.fail();
    } catch (BadRequestException e) {
      // expected
    }

    // search with bad sort order
    try {
      searchMetadata(namespace, "*", targets, MetadataConstants.ENTITY_NAME_KEY + " unknown");
      Assert.fail();
    } catch (BadRequestException e) {
      // expected
    }

    // search with numCursors for relevance sort
    try {
      searchMetadata(NamespaceId.DEFAULT, "search*", targets, null, 0, Integer.MAX_VALUE, 1, null);
      Assert.fail();
    } catch (BadRequestException e) {
      // expected
    }

    // search with cursor for relevance sort
    try {
      searchMetadata(NamespaceId.DEFAULT, "search*", targets, null, 0, Integer.MAX_VALUE, 0, "cursor");
      Assert.fail();
    } catch (BadRequestException e) {
      // expected
    }

    // search with invalid query
    try {
      searchMetadata(NamespaceId.DEFAULT, "");
      Assert.fail();
    } catch (BadRequestException e) {
      // expected
    }
  }

  @Test
  public void testInvalidParams() throws Exception {
    NamespaceId namespace = new NamespaceId("testInvalidParams");
    namespaceClient.create(new NamespaceMeta.Builder().setName(namespace).build());
    try {
      Set<String> targets = Collections.emptySet();
      searchMetadata(namespace, "text", targets, MetadataConstants.CREATION_TIME_KEY + " desc");
      Assert.fail("Expected not to be able to specify 'query' and 'sort' parameters.");
    } catch (BadRequestException expected) {
      // expected
    }
  }


  @Test
  public void testSearchResultSorting() throws Exception {
    NamespaceId namespace = new NamespaceId("sorting");
    namespaceClient.create(new NamespaceMeta.Builder().setName(namespace).build());

    DatasetId text = namespace.dataset("text");
    DatasetId dataset = namespace.dataset("mydataset");
    DatasetId view = namespace.dataset("view");

    // create entities so system metadata is annotated
    // also ensure that they are created at least 1 ms apart
    DatasetInstanceConfiguration instanceConfig =
      new DatasetInstanceConfiguration(Table.class.getName(), Collections.emptyMap());
    datasetClient.create(text, instanceConfig);
    TimeUnit.MILLISECONDS.sleep(1);
    datasetClient.create(view, instanceConfig);
    TimeUnit.MILLISECONDS.sleep(1);
    datasetClient.create(dataset, instanceConfig);

    // search with bad sort param
    Set<String> targets = Collections.emptySet();

    // test ascending order of entity name
    List<MetadataSearchResultRecord> expected = ImmutableList.of(new MetadataSearchResultRecord(dataset),
                                                                 new MetadataSearchResultRecord(text),
                                                                 new MetadataSearchResultRecord(view));
    Tasks.waitFor(expected,
                  () -> new ArrayList<>(searchMetadata(namespace, "*", targets,
                                                       MetadataConstants.ENTITY_NAME_KEY + " asc")),
                  10, TimeUnit.SECONDS);

    // test descending order of entity name
    expected = ImmutableList.of(
      new MetadataSearchResultRecord(view),
      new MetadataSearchResultRecord(text),
      new MetadataSearchResultRecord(dataset)
    );
    Assert.assertEquals(expected,
                        new ArrayList<>(searchMetadata(namespace, "*", targets,
                                                       MetadataConstants.ENTITY_NAME_KEY + " desc")));

    // test ascending order of creation time
    expected = ImmutableList.of(
      new MetadataSearchResultRecord(text),
      new MetadataSearchResultRecord(view),
      new MetadataSearchResultRecord(dataset)
    );
    Assert.assertEquals(expected,
                        new ArrayList<>(searchMetadata(namespace, "*", targets,
                                                       MetadataConstants.CREATION_TIME_KEY + " asc")));

    // test descending order of creation time
    expected = ImmutableList.of(
      new MetadataSearchResultRecord(dataset),
      new MetadataSearchResultRecord(view),
      new MetadataSearchResultRecord(text)
    );
    Assert.assertEquals(expected,
                        new ArrayList<>(searchMetadata(namespace, "*", targets,
                                                       MetadataConstants.CREATION_TIME_KEY + " desc")));
    // cleanup
    namespaceClient.delete(namespace);
  }

  @Test
  public void testSearchResultPaginationWithTargetType() throws Exception {
    // note that the ordering of the entity creations and the sort param used in this test case matter, in order to
    // reproduce the scenario that caused the issue CDAP-7881
    NamespaceId namespace = new NamespaceId("pagination_with_target_type");
    namespaceClient.create(new NamespaceMeta.Builder().setName(namespace).build());
    DatasetId trackerDataset = namespace.dataset("_auditLog");
    DatasetId mydataset = namespace.dataset("mydataset");

    // the creation order below will determine how we see the entities in search result sorted by entity creation time
    // in ascending order
    datasetClient.create(
      trackerDataset,
      new DatasetInstanceConfiguration(Table.class.getName(), Collections.emptyMap())
    );
    datasetClient.create(
      mydataset,
      new DatasetInstanceConfiguration(Table.class.getName(), Collections.emptyMap())
    );

    // do sorting with creation time here since the testSearchResultPagination does with entity name
    // the sorted result order _auditLog mydataset text2 text1 (ascending: creation from earliest time)
    String sort = MetadataConstants.CREATION_TIME_KEY + " " + SortInfo.SortOrder.ASC;

    // offset 1, limit 2, 2 cursors, should return just the dataset created above other than trackerDataset even
    // though it was created before since showHidden is false and it should not affect pagination
    List<MetadataSearchResultRecord> expectedResults = ImmutableList.of(new MetadataSearchResultRecord(mydataset));
    Tasks.waitFor(true, () -> {
      MetadataSearchResponse searchResponse = searchMetadata(
        namespace, "*", ImmutableSet.of(MetadataEntity.DATASET),
        sort, 0, 2, 2, null);
      return expectedResults.equals(new ArrayList<>(searchResponse.getResults()))
        && searchResponse.getCursors().isEmpty();
    }, 10, TimeUnit.SECONDS);
  }

  @Test
  public void testSearchResultPagination() throws Exception {
    NamespaceId namespace = new NamespaceId("pagination");
    namespaceClient.create(new NamespaceMeta.Builder().setName(namespace).build());

    DatasetId text = namespace.dataset("text");
    DatasetId dataset = namespace.dataset("mydataset");
    DatasetId view = namespace.dataset("view");
    DatasetId tracker = namespace.dataset("_auditLog");

    // create entities so system metadata is annotated
    DatasetInstanceConfiguration instanceConfig =
      new DatasetInstanceConfiguration(Table.class.getName(), Collections.emptyMap());
    datasetClient.create(text, instanceConfig);
    datasetClient.create(view, instanceConfig);
    datasetClient.create(dataset, instanceConfig);
    datasetClient.create(tracker, instanceConfig);

    // search with showHidden to true
    Set<String> targets = Collections.emptySet();
    String sort = MetadataConstants.ENTITY_NAME_KEY + " asc";
    // search to get all the above entities offset 0, limit interger max  and cursors 0
    Tasks.waitFor(true, () -> {
      List<MetadataSearchResultRecord> expectedResults = ImmutableList.of(new MetadataSearchResultRecord(tracker),
                                                                          new MetadataSearchResultRecord(dataset),
                                                                          new MetadataSearchResultRecord(text),
                                                                          new MetadataSearchResultRecord(view));
      MetadataSearchResponse searchResponse = searchMetadata(namespace, "*", targets, sort, 0, Integer.MAX_VALUE, 0,
                                                             null, true);
      return expectedResults.equals(new ArrayList<>(searchResponse.getResults()))
        && searchResponse.getCursors().isEmpty();
    }, 10, TimeUnit.SECONDS);

    // no offset, limit 1, no cursors
    MetadataSearchResponse searchResponse = searchMetadata(namespace, "*", targets, sort, 0, 1, 0, null);
    List<MetadataSearchResultRecord> expectedResults = ImmutableList.of(new MetadataSearchResultRecord(dataset));
    Assert.assertEquals(expectedResults, new ArrayList<>(searchResponse.getResults()));
    Assert.assertEquals(0, searchResponse.getNumCursors());
    Assert.assertTrue(searchResponse.getCursors().isEmpty());
    // no offset, limit 1, 2 cursors, should return 1st result, with 2 cursors
    searchResponse = searchMetadata(namespace, "*", targets, sort, 0, 1, 2, null);
    expectedResults = ImmutableList.of(new MetadataSearchResultRecord(dataset));
    Assert.assertEquals(expectedResults, new ArrayList<>(searchResponse.getResults()));
    Assert.assertEquals(1, searchResponse.getNumCursors());
    Assert.assertEquals(1, searchResponse.getCursors().size());
    // offset 1, limit 1, 2 cursors, should return 2nd result, with only 1 cursor since we don't have enough data
    searchResponse = searchMetadata(namespace, "*", targets, sort, 1, 1, 2, null);
    expectedResults = ImmutableList.of(new MetadataSearchResultRecord(text));
    Assert.assertEquals(expectedResults, new ArrayList<>(searchResponse.getResults()));
    Assert.assertEquals(1, searchResponse.getNumCursors());
    Assert.assertEquals(1, searchResponse.getCursors().size());
    // offset 2, limit 1, 2 cursors, should return 3rd result, with 0 cursors since we don't have enough data
    searchResponse = searchMetadata(namespace, "*", targets, sort, 2, 1, 2, null);
    expectedResults = ImmutableList.of(new MetadataSearchResultRecord(view));
    Assert.assertEquals(expectedResults, new ArrayList<>(searchResponse.getResults()));
    Assert.assertEquals(0, searchResponse.getNumCursors());
    Assert.assertTrue(searchResponse.getCursors().isEmpty());
    // offset 3, limit 1, 2 cursors, should 0 results, with 0 cursors since we don't have enough data
    searchResponse = searchMetadata(namespace, "*", targets, sort, 3, 1, 2, null);
    Assert.assertTrue(searchResponse.getResults().isEmpty());
    Assert.assertEquals(0, searchResponse.getNumCursors());
    Assert.assertTrue(searchResponse.getCursors().isEmpty());
    // no offset, no limit, should return everything
    searchResponse = searchMetadata(namespace, "*", targets, sort, 0, Integer.MAX_VALUE, 4, null);
    expectedResults = ImmutableList.of(
      new MetadataSearchResultRecord(dataset),
      new MetadataSearchResultRecord(text),
      new MetadataSearchResultRecord(view)
    );
    Assert.assertEquals(expectedResults, new ArrayList<>(searchResponse.getResults()));
    Assert.assertEquals(0, searchResponse.getNumCursors());
    Assert.assertTrue(searchResponse.getCursors().isEmpty());

    // cleanup
    namespaceClient.delete(namespace);
  }

  private Set<NamespacedEntityId> getEntities(Set<MetadataSearchResultRecord> results) {
    return Sets.newHashSet(
      results.stream().map(MetadataSearchResultRecord::getEntityId).collect(Collectors.toList())
    );
  }

  private Set<MetadataEntity> getMetadataEntities(Set<MetadataSearchResultRecord> results) {
    return Sets.newHashSet(
      results.stream().map(MetadataSearchResultRecord::getMetadataEntity).collect(Collectors.toList())
    );
  }

  private void assertProgramSystemMetadata(ProgramId programId, String mode,
                                           @Nullable String description,
                                           @Nullable ProfileId profileId) throws Exception {
    ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
      .put(MetadataConstants.ENTITY_NAME_KEY, programId.getEntityName())
      .put(AbstractSystemMetadataWriter.VERSION_KEY, ApplicationId.DEFAULT_VERSION);
    if (description != null) {
      properties.put(MetadataConstants.DESCRIPTION_KEY, description);
    }
    if (profileId != null) {
      properties.put("profile", profileId.getScopedName());
      // need to wait for the profile id to come up since we are updating it asyncly
      Tasks.waitFor(profileId.getScopedName(), () -> getProperties(programId, MetadataScope.SYSTEM).get("profile"),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
    }
    Assert.assertEquals(properties.build(), removeCreationTime(getProperties(programId, MetadataScope.SYSTEM)));
    Set<String> expected = ImmutableSet.of(programId.getType().getPrettyName(), mode);
    if (ProgramType.WORKFLOW == programId.getType()) {
      expected = ImmutableSet.of(programId.getType().getPrettyName(), mode,
                                 AllProgramsApp.NoOpAction.class.getSimpleName(), AllProgramsApp.NoOpMR.NAME);
    }
    Assert.assertEquals(expected, getTags(programId, MetadataScope.SYSTEM));
  }

  private void assertArtifactSearch() throws Exception {
    // add a plugin artifact.
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(ManifestFields.EXPORT_PACKAGE,
                                     AllProgramsApp.AppPlugin.class.getPackage().getName());
    ArtifactId pluginArtifact = NamespaceId.DEFAULT.artifact("plugins", "1.0.0");
    addPluginArtifact(pluginArtifact, AllProgramsApp.AppPlugin.class, manifest, null);

    // search using artifact name
    Set<MetadataSearchResultRecord> expected = ImmutableSet.of(new MetadataSearchResultRecord(pluginArtifact));
    Tasks.waitFor(expected, () -> searchMetadata(NamespaceId.DEFAULT, "plugins"), 10, TimeUnit.SECONDS);

    // search the artifact given a plugin
    Set<MetadataSearchResultRecord> results = searchMetadata(NamespaceId.DEFAULT, AllProgramsApp.PLUGIN_TYPE);
    Assert.assertEquals(expected, results);
    results = searchMetadata(NamespaceId.DEFAULT, AllProgramsApp.PLUGIN_NAME + ":" + AllProgramsApp.PLUGIN_TYPE);
    Assert.assertEquals(expected, results);
    results = searchMetadata(NamespaceId.DEFAULT, AllProgramsApp.PLUGIN_NAME, MetadataEntity.ARTIFACT);
    Assert.assertEquals(expected, results);
    // add a user tag to the application with the same name as the plugin
    addTags(application, ImmutableSet.of(AllProgramsApp.PLUGIN_NAME));
    // search for all entities with plugin name. Should return both artifact and application
    results = searchMetadata(NamespaceId.DEFAULT, AllProgramsApp.PLUGIN_NAME);
    Assert.assertEquals(
      ImmutableSet.of(new MetadataSearchResultRecord(application),
                      new MetadataSearchResultRecord(pluginArtifact)),
      results);
    // search for all entities for a plugin with the plugin name. Should return only the artifact, since for the
    // application, its just a tag, not a plugin
    results = searchMetadata(NamespaceId.DEFAULT, "plugin:" + AllProgramsApp.PLUGIN_NAME + ":*");
    Assert.assertEquals(expected, results);
  }

  private void assertAppSearch(ApplicationId app, ArtifactId artifact) throws Exception {
    // using app name
    ImmutableSet<MetadataSearchResultRecord> expected = ImmutableSet.of(
      new MetadataSearchResultRecord(app));
    Assert.assertEquals(expected, searchMetadata(NamespaceId.DEFAULT, AllProgramsApp.NAME));
    // using artifact name: both app and artifact should match
    Assert.assertEquals(
      ImmutableSet.of(new MetadataSearchResultRecord(app),
                      new MetadataSearchResultRecord(artifact)),
      searchMetadata(NamespaceId.DEFAULT, AllProgramsApp.class.getSimpleName()));
    // using program names
    Assert.assertEquals(expected, searchMetadata(NamespaceId.DEFAULT, AllProgramsApp.NoOpMR.NAME,
                                                 MetadataEntity.APPLICATION));
    Assert.assertEquals(expected, searchMetadata(NamespaceId.DEFAULT, AllProgramsApp.NoOpService.NAME,
                                                 MetadataEntity.APPLICATION));
    Assert.assertEquals(expected, searchMetadata(NamespaceId.DEFAULT, AllProgramsApp.NoOpSpark.NAME,
                                                 MetadataEntity.APPLICATION));
    Assert.assertEquals(expected, searchMetadata(NamespaceId.DEFAULT, AllProgramsApp.NoOpWorker.NAME,
                                                 MetadataEntity.APPLICATION));
    Assert.assertEquals(expected, searchMetadata(NamespaceId.DEFAULT, AllProgramsApp.NoOpWorkflow.NAME,
                                                 MetadataEntity.APPLICATION));
    // using program types
    Assert.assertEquals(
      expected, searchMetadata(NamespaceId.DEFAULT,
                               ProgramType.MAPREDUCE.getPrettyName() + MetadataConstants.KEYVALUE_SEPARATOR + "*",
                               MetadataEntity.APPLICATION));
    Assert.assertEquals(
      ImmutableSet.builder().addAll(expected).add(new MetadataSearchResultRecord(application)).build(),
      searchMetadata(NamespaceId.DEFAULT,
                     ProgramType.SERVICE.getPrettyName() + MetadataConstants.KEYVALUE_SEPARATOR + "*",
                     MetadataEntity.APPLICATION));
    Assert.assertEquals(
      expected, searchMetadata(NamespaceId.DEFAULT,
                               ProgramType.SPARK.getPrettyName() + MetadataConstants.KEYVALUE_SEPARATOR + "*",
                               MetadataEntity.APPLICATION));
    Assert.assertEquals(
      expected, searchMetadata(NamespaceId.DEFAULT,
                               ProgramType.WORKER.getPrettyName() + MetadataConstants.KEYVALUE_SEPARATOR + "*",
                               MetadataEntity.APPLICATION));
    Assert.assertEquals(
      expected, searchMetadata(NamespaceId.DEFAULT,
                               ProgramType.WORKFLOW.getPrettyName() + MetadataConstants.KEYVALUE_SEPARATOR + "*",
                               MetadataEntity.APPLICATION));

    // using schedule
    Assert.assertEquals(expected, searchMetadata(NamespaceId.DEFAULT, AllProgramsApp.SCHEDULE_NAME));
    Assert.assertEquals(expected, searchMetadata(NamespaceId.DEFAULT, "EveryMinute"));
  }

  private void assertProgramSearch(ApplicationId app) throws Exception {
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataSearchResultRecord(app.mr(AllProgramsApp.NoOpMR.NAME)),
        new MetadataSearchResultRecord(app.mr(AllProgramsApp.NoOpMR2.NAME)),
        new MetadataSearchResultRecord(app.workflow(AllProgramsApp.NoOpWorkflow.NAME)),
        new MetadataSearchResultRecord(app.spark(AllProgramsApp.NoOpSpark.NAME)),
        new MetadataSearchResultRecord(NamespaceId.DEFAULT.dataset(AllProgramsApp.DATASET_NAME)),
        new MetadataSearchResultRecord(NamespaceId.DEFAULT.dataset(AllProgramsApp.DATASET_NAME2)),
        new MetadataSearchResultRecord(NamespaceId.DEFAULT.dataset(AllProgramsApp.DATASET_NAME3)),
        new MetadataSearchResultRecord(NamespaceId.DEFAULT.dataset(AllProgramsApp.DATASET_NAME4)),
        new MetadataSearchResultRecord(NamespaceId.DEFAULT.dataset(AllProgramsApp.DATASET_NAME5)),
        new MetadataSearchResultRecord(NamespaceId.DEFAULT.dataset(AllProgramsApp.DATASET_NAME6)),
        new MetadataSearchResultRecord(NamespaceId.DEFAULT.dataset(AllProgramsApp.DATASET_NAME7)),
        new MetadataSearchResultRecord(NamespaceId.DEFAULT.dataset(AllProgramsApp.DS_WITH_SCHEMA_NAME)),
        new MetadataSearchResultRecord(myds)
      ),
      searchMetadata(NamespaceId.DEFAULT, "Batch"));
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataSearchResultRecord(app.service(AllProgramsApp.NoOpService.NAME)),
        new MetadataSearchResultRecord(app.worker(AllProgramsApp.NoOpWorker.NAME)),
        new MetadataSearchResultRecord(
          NamespaceId.DEFAULT.app(AppWithDataset.class.getSimpleName()).service("PingService"))
      ),
      searchMetadata(NamespaceId.DEFAULT, "Realtime"));

    // Using program names
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataSearchResultRecord(
          app.mr(AllProgramsApp.NoOpMR.NAME)),
        new MetadataSearchResultRecord(
          app.workflow(AllProgramsApp.NoOpWorkflow.NAME))),
      searchMetadata(NamespaceId.DEFAULT, AllProgramsApp.NoOpMR.NAME, MetadataEntity.PROGRAM));
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataSearchResultRecord(
          app.service(AllProgramsApp.NoOpService.NAME))),
      searchMetadata(NamespaceId.DEFAULT, AllProgramsApp.NoOpService.NAME, MetadataEntity.PROGRAM));
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataSearchResultRecord(
          app.spark(AllProgramsApp.NoOpSpark.NAME))),
      searchMetadata(NamespaceId.DEFAULT, AllProgramsApp.NoOpSpark.NAME, MetadataEntity.PROGRAM));
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataSearchResultRecord(
          app.worker(AllProgramsApp.NoOpWorker.NAME))),
      searchMetadata(NamespaceId.DEFAULT, AllProgramsApp.NoOpWorker.NAME, MetadataEntity.PROGRAM));
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataSearchResultRecord(
          app.workflow(AllProgramsApp.NoOpWorkflow.NAME))),
      searchMetadata(NamespaceId.DEFAULT, AllProgramsApp.NoOpWorkflow.NAME, MetadataEntity.PROGRAM));

    // using program types
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataSearchResultRecord(
          app.mr(AllProgramsApp.NoOpMR.NAME)),
        new MetadataSearchResultRecord(
          app.mr(AllProgramsApp.NoOpMR2.NAME))),
      searchMetadata(NamespaceId.DEFAULT, ProgramType.MAPREDUCE.getPrettyName(), MetadataEntity.PROGRAM));
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataSearchResultRecord(
          app.service(AllProgramsApp.NoOpService.NAME)),
        new MetadataSearchResultRecord(pingService)),
      searchMetadata(NamespaceId.DEFAULT, ProgramType.SERVICE.getPrettyName(), MetadataEntity.PROGRAM));
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataSearchResultRecord(
          app.spark(AllProgramsApp.NoOpSpark.NAME))),
      searchMetadata(NamespaceId.DEFAULT, ProgramType.SPARK.getPrettyName(), MetadataEntity.PROGRAM));
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataSearchResultRecord(
          app.worker(AllProgramsApp.NoOpWorker.NAME))),
      searchMetadata(NamespaceId.DEFAULT, ProgramType.WORKER.getPrettyName(), MetadataEntity.PROGRAM));
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataSearchResultRecord(
          app.workflow(AllProgramsApp.NoOpWorkflow.NAME))),
      searchMetadata(NamespaceId.DEFAULT, ProgramType.WORKFLOW.getPrettyName(), MetadataEntity.PROGRAM));
  }

  private void assertDataEntitySearch() throws Exception {
    DatasetId datasetInstance = NamespaceId.DEFAULT.dataset(AllProgramsApp.DATASET_NAME);
    DatasetId datasetInstance2 = NamespaceId.DEFAULT.dataset(AllProgramsApp.DATASET_NAME2);
    DatasetId datasetInstance3 = NamespaceId.DEFAULT.dataset(AllProgramsApp.DATASET_NAME3);
    DatasetId datasetInstance4 = NamespaceId.DEFAULT.dataset(AllProgramsApp.DATASET_NAME4);
    DatasetId datasetInstance5 = NamespaceId.DEFAULT.dataset(AllProgramsApp.DATASET_NAME5);
    DatasetId datasetInstance6 = NamespaceId.DEFAULT.dataset(AllProgramsApp.DATASET_NAME6);
    DatasetId datasetInstance7 = NamespaceId.DEFAULT.dataset(AllProgramsApp.DATASET_NAME7);
    DatasetId dsWithSchema = NamespaceId.DEFAULT.dataset(AllProgramsApp.DS_WITH_SCHEMA_NAME);

    // schema search for a field with the given fieldname:fieldtype
    Set<MetadataSearchResultRecord> metadataSearchResultRecords =
      searchMetadata(NamespaceId.DEFAULT, "body:STRING+field1:STRING");
    Assert.assertEquals(ImmutableSet.<MetadataSearchResultRecord>builder()
                          .add(new MetadataSearchResultRecord(dsWithSchema))
                          .build(),
                        metadataSearchResultRecords);

    // create a view
    // search all entities that have a defined schema
    // add a user property with "schema" as key
    Map<String, String> datasetProperties = ImmutableMap.of("schema", "schemaValue");
    addProperties(datasetInstance, datasetProperties);

    metadataSearchResultRecords = searchMetadata(NamespaceId.DEFAULT, "schema:*");
    Assert.assertEquals(ImmutableSet.<MetadataSearchResultRecord>builder()
                          .add(new MetadataSearchResultRecord(datasetInstance))
                          .add(new MetadataSearchResultRecord(dsWithSchema))
                          .build(),
                        metadataSearchResultRecords);

    // search dataset
    ImmutableSet<MetadataSearchResultRecord> expectedKvTables = ImmutableSet.of(
      new MetadataSearchResultRecord(datasetInstance),
      new MetadataSearchResultRecord(datasetInstance2),
      new MetadataSearchResultRecord(datasetInstance3),
      new MetadataSearchResultRecord(myds)
    );

    ImmutableSet<MetadataSearchResultRecord> expectedExplorableDatasets =
      ImmutableSet.<MetadataSearchResultRecord>builder()
        .addAll(expectedKvTables)
        .add(new MetadataSearchResultRecord(datasetInstance4))
        .add(new MetadataSearchResultRecord(datasetInstance5))
        .add(new MetadataSearchResultRecord(dsWithSchema))
        .build();
    ImmutableSet<MetadataSearchResultRecord> expectedAllDatasets =
      ImmutableSet.<MetadataSearchResultRecord>builder()
        .addAll(expectedExplorableDatasets)
        .add(new MetadataSearchResultRecord(datasetInstance6))
        .add(new MetadataSearchResultRecord(datasetInstance7))
        .build();
    ImmutableSet<MetadataSearchResultRecord> expectedExplorables =
      ImmutableSet.<MetadataSearchResultRecord>builder()
        .addAll(expectedExplorableDatasets)
        .build();
    metadataSearchResultRecords = searchMetadata(NamespaceId.DEFAULT, "explore");
    Assert.assertEquals(expectedExplorables, metadataSearchResultRecords);
    metadataSearchResultRecords = searchMetadata(NamespaceId.DEFAULT, KeyValueTable.class.getName());
    Assert.assertEquals(expectedKvTables, metadataSearchResultRecords);
    metadataSearchResultRecords = searchMetadata(NamespaceId.DEFAULT, "type:*");
    Assert.assertEquals(expectedAllDatasets, metadataSearchResultRecords);

    metadataSearchResultRecords = searchMetadata(NamespaceId.DEFAULT, AllProgramsApp.DATASET_NAME);
    Assert.assertEquals(ImmutableSet.of(new MetadataSearchResultRecord(datasetInstance)),
                        metadataSearchResultRecords);
    metadataSearchResultRecords = searchMetadata(NamespaceId.DEFAULT, AllProgramsApp.DS_WITH_SCHEMA_NAME);
    Assert.assertEquals(ImmutableSet.of(new MetadataSearchResultRecord(dsWithSchema)),
                        metadataSearchResultRecords);
  }

  private void removeAllMetadata() throws Exception {
    removeMetadata(application);
    removeMetadata(pingService);
    removeMetadata(myds);
    removeMetadata(artifactId);
    removeMetadata(fieldEntity);
  }

  private void assertCleanState() throws Exception {
    assertEmptyMetadata(getMetadata(application.toMetadataEntity(), MetadataScope.USER));
    assertEmptyMetadata(getMetadata(pingService.toMetadataEntity(), MetadataScope.USER));
    assertEmptyMetadata(getMetadata(myds.toMetadataEntity(), MetadataScope.USER));
    assertEmptyMetadata(getMetadata(artifactId.toMetadataEntity(), MetadataScope.USER));
  }

  private void assertEmptyMetadata(Set<MetadataRecord> entityMetadata) {
    // should have two metadata records - one for each scope, both should have empty properties and tags
    Assert.assertEquals(1, entityMetadata.size());
    for (MetadataRecord metadataRecord : entityMetadata) {
      Assert.assertTrue(metadataRecord.getProperties().isEmpty());
      Assert.assertTrue(metadataRecord.getTags().isEmpty());
    }
  }

  /**
   * Returns the artifact id of the deployed application. Need this because we don't know the exact version.
   */
  private ArtifactId getArtifactId() throws Exception {
    Iterable<ArtifactSummary> filtered =
      artifactClient.list(NamespaceId.DEFAULT).stream()
        .filter(artifactSummary -> AllProgramsApp.class.getSimpleName().equals(artifactSummary.getName()))
        .collect(Collectors.toList());
    ArtifactSummary artifact = Iterables.getOnlyElement(filtered);
    return NamespaceId.DEFAULT.artifact(artifact.getName(), artifact.getVersion());
  }

  private Set<MetadataSearchResultRecord> searchMetadata(NamespaceId namespaceId, String query,
                                                         String target) throws Exception {
    return searchMetadata(namespaceId, query, ImmutableSet.of(target));
  }

  private Set<MetadataSearchResultRecord> searchMetadata(NamespaceId namespaceId, String query) throws Exception {
    return searchMetadata(namespaceId, query, ImmutableSet.of());
  }

  /**
   * strips metadata from search results
   */
  @Override
  protected Set<MetadataSearchResultRecord> searchMetadata(NamespaceId namespaceId, String query,
                                                           Set<String> targets) throws Exception {
    return searchMetadata(namespaceId, query, targets, null);
  }

  /**
   * strips metadata from search results
   */
  @Override
  protected Set<MetadataSearchResultRecord> searchMetadata(NamespaceId namespaceId, String query, Set<String> targets,
                                                           @Nullable String sort) throws Exception {
    return searchMetadata(namespaceId, query, targets, sort, 0, Integer.MAX_VALUE, 0, null).getResults();
  }

  /**
   * strips metadata from search results
   */
  @Override
  protected MetadataSearchResponse searchMetadata(NamespaceId namespaceId, String query, Set<String> targets,
                                                  @Nullable String sort, int offset, int limit,
                                                  int numCursors, @Nullable String cursor, boolean showHidden)
    throws Exception {
    MetadataSearchResponse searchResponse = super.searchMetadata(namespaceId, query, targets, sort, offset,
                                                                 limit, numCursors, cursor, showHidden);
    Set<MetadataSearchResultRecord> transformed = new LinkedHashSet<>();
    for (MetadataSearchResultRecord result : searchResponse.getResults()) {
      transformed.add(new MetadataSearchResultRecord(result.getEntityId()));
    }
    return new MetadataSearchResponse(searchResponse.getSort(), searchResponse.getOffset(), searchResponse.getLimit(),
                                      searchResponse.getNumCursors(), searchResponse.getTotal(), transformed,
                                      searchResponse.getCursors(), searchResponse.isShowHidden(),
                                      searchResponse.getEntityScope());
  }

  private MetadataSearchResponse searchMetadata(NamespaceId namespaceId, String query, Set<String> targets,
                                                @Nullable String sort, int offset, int limit,
                                                int numCursors, @Nullable String cursor) throws Exception {
    return searchMetadata(namespaceId, query, targets, sort, offset, limit, numCursors, cursor, false);
  }

  private Set<MetadataRecord> removeCreationTime(Set<MetadataRecord> original) {
    MetadataRecord systemRecord = null;
    for (MetadataRecord record : original) {
      if (MetadataScope.SYSTEM == record.getScope()) {
        systemRecord = record;
      }
    }
    Assert.assertNotNull(systemRecord);
    removeCreationTime(systemRecord.getProperties());
    return original;
  }

  private Map<String, String> removeCreationTime(Map<String, String> systemProperties) {
    Assert.assertTrue(systemProperties.containsKey(MetadataConstants.CREATION_TIME_KEY));
    long createTime = Long.parseLong(systemProperties.get(MetadataConstants.CREATION_TIME_KEY));
    Assert.assertTrue("Create time should be within the last hour - " + createTime,
                      createTime > System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1));
    systemProperties.remove(MetadataConstants.CREATION_TIME_KEY);
    return systemProperties;
  }
}
