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

package co.cask.cdap.internal.app.runtime.artifact;

import co.cask.cdap.common.ArtifactNotFoundException;
import co.cask.cdap.proto.id.ArtifactId;
import org.apache.twill.filesystem.Location;

import java.io.IOException;

/**
 * Interface for finding plugin.
 */
public interface ArtifactFinder {

  /**
   * Finds the location of an artifact.
   *
   * @param artifactId the artifact id
   * @return the location of the artifact.
   * @throws ArtifactNotFoundException if no plugin can be found
   */
  Location getArtifactLocation(ArtifactId artifactId) throws ArtifactNotFoundException, IOException;

}
