/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

package co.cask.cdap.metadata;

import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.api.metadata.MetadataScope;
import co.cask.cdap.client.MetadataClient;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.metadata.MetadataRecord;
import co.cask.cdap.common.security.AuditDetail;
import co.cask.cdap.common.security.AuditPolicy;
import co.cask.cdap.data2.metadata.dataset.SearchRequest;
import co.cask.cdap.data2.metadata.dataset.SortInfo;
import co.cask.cdap.proto.EntityScope;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.codec.NamespacedEntityIdCodec;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.proto.metadata.MetadataSearchResponse;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Type;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * HttpHandler for Metadata
 */
@Path(Constants.Gateway.API_VERSION_3)
public class MetadataHttpHandler extends AbstractHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(MetadataHttpHandler.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(NamespacedEntityId.class, new NamespacedEntityIdCodec())
    .create();
  private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final Type SET_STRING_TYPE = new TypeToken<Set<String>>() { }.getType();
  private static final Type SET_METADATA_RECORD_TYPE = new TypeToken<Set<MetadataRecord>>() { }.getType();

  private final MetadataAdmin metadataAdmin;
  private final LineageAdmin lineageAdmin;

  @Inject
  MetadataHttpHandler(MetadataAdmin metadataAdmin, LineageAdmin lineageAdmin) {
    this.metadataAdmin = metadataAdmin;
    this.lineageAdmin = lineageAdmin;
  }

  @GET
  @Path("/**/metadata")
  public void getMetadata(HttpRequest request, HttpResponder responder,
                          @QueryParam("scope") String scope, @QueryParam("type") String type,
                          @DefaultValue("true") @QueryParam("aggregateRun") boolean aggregateRun)
    throws BadRequestException {
    MetadataEntity metadataEntity = getMetadataEntityFromPath(request.uri(), type, "/metadata");
    Set<MetadataRecord> metadata = getMetadata(metadataEntity, scope, aggregateRun);
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(metadata, SET_METADATA_RECORD_TYPE));
  }

  @GET
  @Path("/**/metadata/properties")
  public void getProperties(HttpRequest request, HttpResponder responder,
                            @QueryParam("scope") String scope, @QueryParam("type") String type)
    throws BadRequestException {
    MetadataEntity metadataEntity = getMetadataEntityFromPath(request.uri(), type, "/metadata/properties");
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(getProperties(metadataEntity, scope)));
  }

  @GET
  @Path("/**/metadata/tags")
  public void getTags(HttpRequest request, HttpResponder responder,
                      @QueryParam("scope") String scope, @QueryParam("type") String type)
    throws BadRequestException {
    MetadataEntity metadataEntity = getMetadataEntityFromPath(request.uri(), type, "/metadata/tags");
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(getTags(metadataEntity, scope)));
  }

  @POST
  @Path("/**/metadata/properties")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void addProperties(FullHttpRequest request, HttpResponder responder,
                            @QueryParam("type") String type) throws BadRequestException {
    MetadataEntity metadataEntity = getMetadataEntityFromPath(request.uri(), type, "/metadata/properties");
    metadataAdmin.addProperties(metadataEntity, readProperties(request));
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Metadata properties for %s added successfully.", metadataEntity));
  }

  @POST
  @Path("/**/metadata/tags")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void addTags(FullHttpRequest request, HttpResponder responder,
                      @QueryParam("type") String type) throws BadRequestException {
    MetadataEntity metadataEntity = getMetadataEntityFromPath(request.uri(), type, "/metadata/tags");
    metadataAdmin.addTags(metadataEntity, readTags(request));
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Metadata tags for %s added successfully.", metadataEntity));
  }

  @DELETE
  @Path("/**/metadata")
  public void removeMetadata(HttpRequest request, HttpResponder responder,
                             @QueryParam("type") String type) {
    MetadataEntity metadataEntity = getMetadataEntityFromPath(request.uri(), type, "/metadata");
    metadataAdmin.removeMetadata(metadataEntity);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Metadata for %s deleted successfully.", metadataEntity));
  }

  @DELETE
  @Path("/**/metadata/properties")
  public void removeProperties(HttpRequest request, HttpResponder responder,
                               @QueryParam("type") String type) {
    MetadataEntity metadataEntity = getMetadataEntityFromPath(request.uri(), type, "/metadata/properties");
    metadataAdmin.removeProperties(metadataEntity);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Metadata properties for %s deleted successfully.", metadataEntity));
  }

  @DELETE
  @Path("/**/properties/{property}")
  public void removeProperty(HttpRequest request, HttpResponder responder,
                             @PathParam("property") String property,
                             @QueryParam("type") String type) {
    MetadataEntity metadataEntity = getMetadataEntityFromPath(request.uri(), type, "/metadata/properties");
    metadataAdmin.removeProperties(metadataEntity, Collections.singleton(property));
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Metadata property %s for %s deleted successfully.", property, metadataEntity));
  }

  @DELETE
  @Path("/**/metadata/tags")
  public void removeTags(HttpRequest request, HttpResponder responder,
                         @QueryParam("type") String type) {
    MetadataEntity metadataEntity = getMetadataEntityFromPath(request.uri(), type, "/metadata/tags");
    metadataAdmin.removeTags(metadataEntity);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Metadata tags for %s deleted successfully.", metadataEntity));
  }

  @DELETE
  @Path("/**/metadata/tags/{tag}")
  public void removeTag(HttpRequest request, HttpResponder responder,
                        @PathParam("tag") String tag,
                        @QueryParam("type") String type) {
    MetadataEntity metadataEntity = getMetadataEntityFromPath(request.uri(), type, "/metadata/tags");
    metadataAdmin.removeTags(metadataEntity, Collections.singleton(tag));
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Metadata tag %s for %s deleted successfully.", tag, metadataEntity));
  }

  @GET
  @Path("/metadata/search")
  public void searchMetadata(HttpRequest request, HttpResponder responder,
                             @Nullable @QueryParam("query") String searchQuery,
                             @Nullable @QueryParam("target") List<String> targets,
                             @QueryParam("sort") @DefaultValue("") String sort,
                             @QueryParam("offset") @DefaultValue("0") int offset,
                             // 2147483647 is Integer.MAX_VALUE
                             @QueryParam("limit") @DefaultValue("2147483647") int limit,
                             @QueryParam("numCursors") @DefaultValue("0") int numCursors,
                             @QueryParam("cursor") @DefaultValue("") String cursor,
                             @QueryParam("showHidden") @DefaultValue("false") boolean showHidden,
                             @Nullable @QueryParam("entityScope") String entityScope) throws Exception {
    SearchRequest searchRequest = getValidatedSearchRequest(null, searchQuery, targets, sort, offset,
                                                            limit, numCursors, cursor, showHidden, entityScope);

    MetadataSearchResponse response = metadataAdmin.search(searchRequest);
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(response, MetadataSearchResponse.class));
  }

  @GET
  @Path("/namespaces/{namespace-id}/metadata/search")
  public void searchMetadata(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") String namespaceId,
                             @Nullable @QueryParam("query") String searchQuery,
                             @Nullable @QueryParam("target") List<String> targets,
                             @QueryParam("sort") @DefaultValue("") String sort,
                             @QueryParam("offset") @DefaultValue("0") int offset,
                             // 2147483647 is Integer.MAX_VALUE
                             @QueryParam("limit") @DefaultValue("2147483647") int limit,
                             @QueryParam("numCursors") @DefaultValue("0") int numCursors,
                             @QueryParam("cursor") @DefaultValue("") String cursor,
                             @QueryParam("showHidden") @DefaultValue("false") boolean showHidden,
                             @Nullable @QueryParam("entityScope") String entityScope) throws Exception {
    SearchRequest searchRequest = getValidatedSearchRequest(namespaceId, searchQuery, targets, sort, offset,
                                                            limit, numCursors, cursor, showHidden, entityScope);

    MetadataSearchResponse response = metadataAdmin.search(searchRequest);
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(response, MetadataSearchResponse.class));
  }

  private SearchRequest getValidatedSearchRequest(@Nullable String namespace, @Nullable String searchQuery,
                                                  @Nullable List<String> targets, String sort, int offset, int limit,
                                                  int numCursors, String cursor, boolean showHidden, String entityScope)
    throws BadRequestException, UnsupportedEncodingException {

    Set<String> types = targets == null ? Collections.emptySet() : ImmutableSet.copyOf(targets);

    SortInfo sortInfo = SortInfo.of(URLDecoder.decode(sort, StandardCharsets.UTF_8.name()));
    if (SortInfo.DEFAULT.equals(sortInfo)) {
      if (!(cursor.isEmpty()) || 0 != numCursors) {
        throw new BadRequestException("Cursors are not supported when sort info is not specified.");
      }
    }

    NamespaceId namespaceId;
    try {
      namespaceId = namespace == null ? null : new NamespaceId(namespace);
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(e.getMessage(), e);
    }

    try {
      SearchRequest request = new SearchRequest(namespaceId, searchQuery, types, sortInfo, offset, limit, numCursors,
                                                cursor, showHidden, validateEntityScope(entityScope));
      LOG.trace("Received search request {}", request);
      return request;
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(e.getMessage());
    }
  }

  private MetadataEntity getMetadataEntityFromPath(String uri, @Nullable  String entityType, String suffix) {
    String[] parts = uri.substring((uri.indexOf(Constants.Gateway.API_VERSION_3) +
      Constants.Gateway.API_VERSION_3.length() + 1), uri.lastIndexOf(suffix)).split("/");
    MetadataEntity.Builder builder = MetadataEntity.builder();

    int curIndex = 0;
    while (curIndex < parts.length - 1) {
      // the api part which we get for program does not have 'type' keyword as part of the uri. It goes like
      // ../apps/appName/programType/ProgramName so handle that correctly
      if (curIndex >= 2 && parts[curIndex - 2].equalsIgnoreCase("apps") && !parts[curIndex].equalsIgnoreCase
        ("versions")) {
        builder = appendHelper(builder, entityType, MetadataEntity.TYPE,
                                      ProgramType.valueOfCategoryName(parts[curIndex]).name());
        builder = appendHelper(builder, entityType, MetadataEntity.PROGRAM, parts[curIndex + 1]);
      } else {
        if (MetadataClient.ENTITY_TYPE_TO_API_PART.inverse().containsKey(parts[curIndex])) {
          builder = appendHelper(builder, entityType,
                                        MetadataClient.ENTITY_TYPE_TO_API_PART.inverse().get(parts[curIndex]),
                                        parts[curIndex + 1]);
        } else {
          builder = appendHelper(builder, entityType, parts[curIndex], parts[curIndex + 1]);
        }
      }
      curIndex += 2;
    }
    builder = makeBackwardCompatible(builder);
    return builder.build();
  }

  /**
   * If the MetadataEntity Builder key-value represent an application or artifact which is versioned in CDAP i.e. their
   * metadata entity representation ends with 'version' rather than 'application' or 'artifact' and the type is also
   * set to 'version' then for backward compatibility of rest end points return an updated builder which has the
   * correct type. This is only needed in 5.0 for backward compatibility of the rest endpoint.
   * From 5.0 and later the rest end point must be called with a query parameter which specify the type of the
   * metadata entity if the type is not the last key. (CDAP-13678)
   */
  @VisibleForTesting
  static MetadataEntity.Builder makeBackwardCompatible(MetadataEntity.Builder builder) {
    MetadataEntity entity = builder.build();
    List<MetadataEntity.KeyValue> entityKeyValues = StreamSupport.stream(entity.spliterator(), false)
      .collect(Collectors.toList());
    if (entityKeyValues.size() == 3 && entity.getType().equals(MetadataEntity.VERSION) &&
      (entityKeyValues.get(1).getKey().equals(MetadataEntity.ARTIFACT) ||
        entityKeyValues.get(1).getKey().equals(MetadataEntity.APPLICATION))) {
      // this is artifact or application so update the builder
      MetadataEntity.Builder actualEntityBuilder = MetadataEntity.builder();
      // namespace
      actualEntityBuilder.append(entityKeyValues.get(0).getKey(), entityKeyValues.get(0).getValue());
      // application or artifact (so append as type)
      actualEntityBuilder.appendAsType(entityKeyValues.get(1).getKey(), entityKeyValues.get(1).getValue());
      // version detail
      actualEntityBuilder.append(entityKeyValues.get(2).getKey(), entityKeyValues.get(2).getValue());
      return actualEntityBuilder;
    }
    return builder;
  }

  private MetadataEntity.Builder appendHelper(MetadataEntity.Builder builder, @Nullable String entityType,
                                      String key, String value) {

    if (entityType == null || entityType.isEmpty()) {
      // if a type is not provided then keep appending as type to update the type on every append
      return builder.appendAsType(key, value);
    } else {
      if (entityType.equalsIgnoreCase(key)) {
        // if a type was provided and this key is the type then appendAsType
        return builder.appendAsType(key, value);
      } else {
        return builder.append(key, value);
      }
    }
  }

  private Map<String, String> readProperties(FullHttpRequest request) throws BadRequestException {
    ByteBuf content = request.content();
    if (!content.isReadable()) {
      throw new BadRequestException("Unable to read metadata properties from the request.");
    }

    Map<String, String> metadata;
    try (Reader reader = new InputStreamReader(new ByteBufInputStream(content), StandardCharsets.UTF_8)) {
      metadata = GSON.fromJson(reader, MAP_STRING_STRING_TYPE);
    } catch (IOException e) {
      throw new BadRequestException("Unable to read metadata properties from the request.", e);
    }

    if (metadata == null) {
      throw new BadRequestException("Null metadata was read from the request");
    }
    return metadata;
  }

  private Set<String> readTags(FullHttpRequest request) throws BadRequestException {
    ByteBuf content = request.content();
    if (!content.isReadable()) {
      throw new BadRequestException("Unable to read a list of tags from the request.");
    }
    Set<String> toReturn;
    try (Reader reader = new InputStreamReader(new ByteBufInputStream(content), StandardCharsets.UTF_8)) {
      toReturn = GSON.fromJson(reader, SET_STRING_TYPE);
    } catch (IOException e) {
      throw new BadRequestException("Unable to read a list of tags from the request.", e);
    }

    if (toReturn == null) {
      throw new BadRequestException("Null tags were read from the request.");
    }
    return toReturn;
  }

  private Set<MetadataRecord> getMetadata(MetadataEntity metadataEntity,
                                          @Nullable String scope, boolean aggregateRun) throws BadRequestException {
    // the lineage admin handles the metadata call for program runs so delegate the call to that
    Set<MetadataRecord> metadata;
    if (aggregateRun && metadataEntity.getType().equals(MetadataEntity.PROGRAM_RUN)) {
      // CDAP-13721 for backward compatibility we need to support metadata aggregation of runId.
      metadata = lineageAdmin.getMetadataForRun(EntityId.fromMetadataEntity(metadataEntity));
    } else {
      if (scope == null) {
        metadata = metadataAdmin.getMetadata(metadataEntity);
      } else {
        metadata = metadataAdmin.getMetadata(validateScope(scope), metadataEntity);
      }
    }
    return metadata;
  }

  private Map<String, String> getProperties(MetadataEntity metadataEntity,
                                            @Nullable String scope) throws BadRequestException {
    return (scope == null) ?
      metadataAdmin.getProperties(metadataEntity) :
      metadataAdmin.getProperties(validateScope(scope), metadataEntity);
  }

  private Set<String> getTags(MetadataEntity metadataEntity,
                              @Nullable String scope) throws BadRequestException {
    return (scope == null) ?
      metadataAdmin.getTags(metadataEntity) :
      metadataAdmin.getTags(validateScope(scope), metadataEntity);
  }

  private MetadataScope validateScope(String scope) throws BadRequestException {
    try {
      return MetadataScope.valueOf(scope.toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(String.format("Invalid metadata scope '%s'. Expected '%s' or '%s'",
                                                  scope, MetadataScope.USER, MetadataScope.SYSTEM));
    }
  }

  private Set<EntityScope> validateEntityScope(@Nullable String entityScope) throws BadRequestException {
    if (entityScope == null) {
      return EnumSet.allOf(EntityScope.class);
    }

    try {
      return EnumSet.of(EntityScope.valueOf(entityScope.toUpperCase()));
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(String.format("Invalid entity scope '%s'. Expected '%s' or '%s' for entities " +
                                                    "from specified scope, or just omit the parameter to get " +
                                                    "entities from both scopes",
                                                  entityScope, EntityScope.USER, EntityScope.SYSTEM));
    }
  }
}
