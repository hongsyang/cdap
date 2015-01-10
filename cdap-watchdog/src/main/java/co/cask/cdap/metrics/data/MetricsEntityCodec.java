/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.metrics.data;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.common.utils.ImmutablePair;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Maps;

import java.util.Arrays;
import java.util.EnumMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * Class for encode/decode metric entities (context, metric and tag).
 */
final class MetricsEntityCodec {

  private static final long CACHE_EXPIRE_MINUTE = 10;
  private static final Pattern ENTITY_SPLITTER = Pattern.compile("\\.");
  private static final String[] EMPTY_STRINGS = new String[0];

  private final EntityTable entityTable;
  private final int contextDepth;
  private final int metricDepth;
  private final int tagDepth;
  private final EnumMap<MetricsEntityType, LoadingCache<String, byte[]>> entityCaches;


  MetricsEntityCodec(EntityTable entityTable, int contextDepth, int metricDepth, int tagDepth) {
    this.entityTable = entityTable;
    this.contextDepth = contextDepth;
    this.metricDepth = metricDepth;
    this.tagDepth = tagDepth;

    this.entityCaches = Maps.newEnumMap(MetricsEntityType.class);
    for (MetricsEntityType type : MetricsEntityType.values()) {
      LoadingCache<String, byte[]> cache = CacheBuilder.newBuilder()
                                                       .expireAfterAccess(CACHE_EXPIRE_MINUTE, TimeUnit.MINUTES)
                                                       .build(createCacheLoader(type));
      this.entityCaches.put(type, cache);
    }
  }

  /**
   * Encodes a '.' separated entity into bytes.
   * @param type Type of the entity.
   * @param entity Value of the entity.
   * @return byte[] representing the given entity.
   */
  public byte[] encode(MetricsEntityType type, String entity) {
    return entityCaches.get(type).getUnchecked(entity);
  }

  /**
   * Encodes a dot separated entity into bytes without padding.
   * @param type Type of the entity.
   * @param entity Value of the entity.
   * @return byte[] representing the given entity.
   */
  public byte[] encodeWithoutPadding(MetricsEntityType type, String entity) {
    int idSize = entityTable.getIdSize();
    String[] entityParts = entity == null ? EMPTY_STRINGS : ENTITY_SPLITTER.split(entity);
    byte[] result = new byte[entityParts.length * idSize];

    for (int i = 0; i < entityParts.length; i++) {
      idToBytes(entityTable.getId(type.getType() + i, entityParts[i]), idSize, result, i * idSize);
    }
    return result;
  }

  /**
   * The entity string is split by {@link java.util.regex.Pattern}ENTITY_SPLITTER
   * and return the length of this split array.
   */
  public int getEntityPartsLength(@Nullable String entity) {
    return entity == null ? 0 : ENTITY_SPLITTER.split(entity).length;
  }

  /**
   * Encodes a '.' separated entity into bytes. If the entity has less than the given parts or {@code null},
   * the remaining bytes would be padded by the given padding.
   * @param type Type of the entity.
   * @param entity Value of the entity.
   * @param padding Padding byte to apply for padding. Eg: typically start rows use 0 padding and
   *                end rows use '0xff' padding for getting metric-entity byte array,
   *                the extra bytes in the end are filled with this padding value.
   * @return byte[] representing the given entity that may have padding at the end.
   */
  public byte[] paddedEncode(MetricsEntityType type, String entity, int padding) {
    int idSize = entityTable.getIdSize();
    int depth = getDepth(type);

    byte[] result = new byte[depth * idSize];
    paddedEncode(type, entity, padding, result, 0);
    return  result;
  }

  private void paddedEncode(MetricsEntityType type, String entity, int padding, byte[] result, int offset) {
    int idSize = entityTable.getIdSize();
    int depth = getDepth(type);
    String[] entityParts = entity == null ? EMPTY_STRINGS : ENTITY_SPLITTER.split(entity, depth);
    for (int i = 0; i < entityParts.length; i++) {
      if (entityParts[i].isEmpty()) {
        throw new IllegalArgumentException("found empty part in metrics entity " + entity);
      }
      idToBytes(entityTable.getId(type.getType() + i, entityParts[i]), idSize, result, i * idSize + offset);
    }

    Arrays.fill(result, entityParts.length * idSize + offset, depth * idSize + offset, (byte) (padding & 0xff));
  }

  /**
   * The rowkey consists of the following params in that order,
   * while padding parameter is used to apply padding during individual parameter encoding.
   * @param contextPrefix Context the metric belongs to.
   * @param metricPrefix  metric string
   * @param tagPrefix metric tag.
   * @param timeBase timeBase.
   * @param runId runId if the metric belongs to a program.
   * @param padding Padding byte to apply for padding.
   * @return byte[] representing the rowkey that may have padding for each part.
   */
  public byte[] paddedEncode(String contextPrefix,  String metricPrefix, String tagPrefix,
                             int timeBase, String runId, int padding) {
    int idSize = entityTable.getIdSize();
    int totalDepth = getDepth(MetricsEntityType.CONTEXT) + getDepth(MetricsEntityType.METRIC) +
      getDepth(MetricsEntityType.TAG) + getDepth(MetricsEntityType.RUN);
    int sizeOfTimeBase = 4;
    byte[] result = new byte[idSize * totalDepth + sizeOfTimeBase];
    int offset = 0;
    paddedEncode(MetricsEntityType.CONTEXT, contextPrefix, padding, result, offset);
    offset += idSize * getDepth(MetricsEntityType.CONTEXT);
    paddedEncode(MetricsEntityType.METRIC, metricPrefix, padding, result, offset);
    offset += idSize * getDepth(MetricsEntityType.METRIC);
    paddedEncode(MetricsEntityType.TAG, tagPrefix, padding, result, offset);
    offset += idSize * getDepth(MetricsEntityType.TAG);
    System.arraycopy(Bytes.toBytes(timeBase), 0 , result, offset, sizeOfTimeBase);
    offset += sizeOfTimeBase;
    paddedEncode(MetricsEntityType.RUN, runId, padding, result, offset);
    return result;
  }

  /**
   * Encodes a '.' separated entity into bytes. If the entity has less than the given parts, the remaining bytes
   * would be padded by the given padding. Also return a fuzzy mask with byte value = 1 for the padded bytes and
   * 0 for non padded bytes.
   *
   * @param type Type of the entity.
   * @param entity Value of the entity.
   * @param padding Padding byte to apply for padding.
   * @return ImmutablePair with first byte[] representing the given entity that may have padding at the end and
   *         second byte[] as the fuzzy mask.
   */
  public ImmutablePair<byte[], byte[]> paddedFuzzyEncode(MetricsEntityType type, String entity, int padding) {
    int idSize = entityTable.getIdSize();
    int depth = getDepth(type);
    String[] entityParts = entity == null ? EMPTY_STRINGS : ENTITY_SPLITTER.split(entity, depth);
    byte[] result = new byte[depth * idSize];
    byte[] mask = new byte[depth * idSize];

    Arrays.fill(mask, (byte) 0);

    for (int i = 0; i < entityParts.length; i++) {
      idToBytes(entityTable.getId(type.getType() + i, entityParts[i]), idSize, result, i * idSize);
    }

    Arrays.fill(result, entityParts.length * idSize, depth * idSize, (byte) (padding & 0xff));
    Arrays.fill(mask, entityParts.length * idSize, depth * idSize, (byte) 1);

    return new ImmutablePair<byte[], byte[]>(result, mask);
  }

  public String decode(MetricsEntityType type, byte[] encoded) {
    return decode(type, encoded, 0);
  }

  /**
   * Decodes a byte[] into '.' separated entity string.
   */
  public String decode(MetricsEntityType type, byte[] encoded, int offset) {
    StringBuilder builder = new StringBuilder();
    int idSize = entityTable.getIdSize();
    int length = getDepth(type);
    Preconditions.checkArgument(length > 0, "Too few bytes to decode.");

    for (int i = 0; i < length; i++) {
      long id = decodeId(encoded, offset + i * idSize, idSize);
      if (id == 0) {
        // It's the padding byte, break the loop.
        break;
      }
      builder.append(entityTable.getName(id, type.getType() + i))
             .append('.');
    }

    builder.setLength(builder.length() - 1);
    return builder.toString();
  }

  /**
   * Returns the number of bytes that the given entity type would occupy.
   */
  public int getEncodedSize(MetricsEntityType type) {
    return getDepth(type) * entityTable.getIdSize();
  }

  public int getIdSize() {
    return entityTable.getIdSize();
  }

  /**
   * Creates a CacheLoader for entity name to encoded byte[].
   * @param type Type of the entity.
   */
  private CacheLoader<String, byte[]> createCacheLoader(final MetricsEntityType type) {
    return new CacheLoader<String, byte[]>() {
      @Override
      public byte[] load(String key) throws Exception {
        return paddedEncode(type, key, 0);
      }
    };
  }

  private int getDepth(MetricsEntityType type) {
    switch (type) {
      case CONTEXT:
        return contextDepth;
      case RUN:
        return 1;   // RunId doesn't have hierarchy
      case METRIC:
        return metricDepth;
      case TAG:
        return tagDepth;
    }
    throw new IllegalArgumentException("Unsupported entity type: " + type);
  }

  /**
   * Save a long id into the given byte array, assuming the given array is always big enough.
   */
  private void idToBytes(long id, int idSize, byte[] bytes, int offset) {
    while (idSize != 0) {
      idSize--;
      bytes[offset + idSize] = (byte) (id & 0xff);
      id >>= 8;
    }
  }

  private long decodeId(byte[] bytes, int offset, int idSize) {
    long id = 0;
    for (int i = 0; i < idSize; i++) {
      id |= (bytes[offset + i] & 0xff) << ((idSize - i - 1) * 8);
    }
    return id;
  }
}
