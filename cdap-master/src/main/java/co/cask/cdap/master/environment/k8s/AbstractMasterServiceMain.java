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

package co.cask.cdap.master.environment.k8s;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Injector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;

/**
 * Abstract service main for CDAP master services.
 */
public abstract class AbstractMasterServiceMain extends AbstractServiceMain<EnvironmentOptions> {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractMasterServiceMain.class);

  @Override
  protected EnvironmentOptions createOptions() {
    return new EnvironmentOptions();
  }

  @Override
  public void init(String[] args) throws MalformedURLException {
    LOG.info("Initializing master service class {}", getClass().getName());
    super.init(args);
    LOG.info("Master service {} initialized", getClass().getName());
  }

  @Override
  public void start() {
    LOG.info("Starting all services for master service {}", getClass().getName());
    startServices();
    LOG.info("All services for master service {} started", getClass().getName());
  }

  @Override
  public void stop() {
    // Stop service in reverse order
    LOG.info("Stopping all services for master service {}", getClass().getName());
    stopServices();
    LOG.info("All services for master service {} stopped", getClass().getName());
  }

  @VisibleForTesting
  Injector getInjector() {
    return injector;
  }
}
