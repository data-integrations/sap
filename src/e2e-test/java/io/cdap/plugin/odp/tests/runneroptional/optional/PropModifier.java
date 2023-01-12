/*
 * Copyright © 2021 Cask Data, Inc.
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
package io.cdap.plugin.odp.tests.runneroptional.optional;

import cucumber.api.event.EventListener;
import cucumber.api.event.EventPublisher;
import io.cdap.plugin.odp.utils.CDAPUtils;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Event listener to update properties on the run, called as a plugin by TestRunner.
 */
public class PropModifier implements EventListener {

  private static final Logger logger = Logger.getLogger(PropModifier.class);

  private static Properties pluginPropBackup;

  public PropModifier(String fileRelativePath) {
    appendToProps(fileRelativePath);
  }

  private void appendToProps(String fileRelativePath) {
    try {
      pluginPropBackup = (Properties) CDAPUtils.pluginProp.clone();
      CDAPUtils.pluginProp.load(new FileInputStream(fileRelativePath));
    } catch (IOException e) {
      logger.error("Error while reading file: " + e);
    }
  }

  @Override
  public void setEventPublisher(EventPublisher eventPublisher) {
    //post action: reset properties to default
    CDAPUtils.pluginProp = pluginPropBackup;
  }
}
