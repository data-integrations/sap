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
package io.cdap.plugin.odp.actions;

import io.cdap.e2e.utils.SeleniumDriver;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.odp.locators.ODPLocators;
import io.cdap.plugin.odp.utils.CDAPUtils;
import org.apache.commons.lang3.StringUtils;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.support.PageFactory;

import java.io.IOException;
import java.util.UUID;

/**
 * ODPActions.
 */
public class ODPActions {
  public static ODPLocators odpLocators;
  private static JavascriptExecutor js;

  static {
    js = (JavascriptExecutor) SeleniumDriver.getDriver();
    odpLocators = SeleniumHelper.getPropertiesLocators(ODPLocators.class);
  }

  public static void selectODPSource() {
    odpLocators.sapODPSource.click();
  }

  public static void getODPProperties() {
    odpLocators.sapODPProperties.click();
  }

  public static void enterConnectionProperties(
    String client, String sysnr, String asHost, String dsName, String gcsPath, String splitRow,
    String packSize, String msServ, String systemID, String lgrp) throws IOException, InterruptedException {
    odpLocators.referenceName.sendKeys(UUID.randomUUID().toString());
    odpLocators.sapClient.sendKeys(CDAPUtils.getPluginProp(client));
    if (null != CDAPUtils.getPluginProp(msServ)) {
      /*For load connection*/
      odpLocators.loadServer.click();
      odpLocators.msHost.sendKeys(null != CDAPUtils.getPluginProp(asHost) ?
                                    CDAPUtils.getPluginProp(asHost) : StringUtils.EMPTY);
      odpLocators.portNumber.sendKeys(null != CDAPUtils.getPluginProp(msServ) ?
                                        CDAPUtils.getPluginProp(msServ) : StringUtils.EMPTY);
      odpLocators.systemID.sendKeys(null != CDAPUtils.getPluginProp(systemID) ?
                                      CDAPUtils.getPluginProp(systemID) : StringUtils.EMPTY);
      odpLocators.logonGroup.sendKeys(null != CDAPUtils.getPluginProp(lgrp) ?
                                        CDAPUtils.getPluginProp(lgrp) : StringUtils.EMPTY);
    } else {
      /*For direct connection*/
      odpLocators.systemNumber.sendKeys(CDAPUtils.getPluginProp(sysnr));
      odpLocators.sapApplicationServerHost.sendKeys(CDAPUtils.getPluginProp(asHost));
    }
    odpLocators.dataSourceName.sendKeys(CDAPUtils.getPluginProp(dsName));
    js.executeScript("window.scrollBy(0,350)", StringUtils.EMPTY);
    odpLocators.gcsPath.sendKeys(CDAPUtils.getPluginProp(gcsPath));
    odpLocators.splitRow.sendKeys(CDAPUtils.getPluginProp(splitRow));
    odpLocators.packageSize.sendKeys(null != CDAPUtils.getPluginProp(packSize) ?
                                       CDAPUtils.getPluginProp(packSize) : StringUtils.EMPTY);
  }

  public static void enterDirectConnectionProperties(String client, String sysnr, String asHost, String dsName
    , String gcsPath, String splitRow, String packSize) throws IOException, InterruptedException {
    odpLocators.referenceName.sendKeys(UUID.randomUUID().toString());
    odpLocators.sapClient.sendKeys(CDAPUtils.getPluginProp(client));
    odpLocators.systemNumber.sendKeys(CDAPUtils.getPluginProp(sysnr));
    odpLocators.sapApplicationServerHost.sendKeys(CDAPUtils.getPluginProp(asHost));
    odpLocators.dataSourceName.sendKeys(CDAPUtils.getPluginProp(dsName));
    js.executeScript("window.scrollBy(0,350)", StringUtils.EMPTY);
    odpLocators.gcsPath.sendKeys(CDAPUtils.getPluginProp(gcsPath));
    odpLocators.splitRow.sendKeys(CDAPUtils.getPluginProp(splitRow));
    odpLocators.packageSize.sendKeys(null != CDAPUtils.getPluginProp(packSize) ?
                                       CDAPUtils.getPluginProp(packSize) : StringUtils.EMPTY);
  }

  public static void enterLoadConnectionProperties(String client, String asHost, String msServ, String systemID,
                                                   String lgrp, String dsName, String gcsPath, String splitrow,
                                                   String packageSize) throws IOException, InterruptedException {
    odpLocators.referenceName.sendKeys(UUID.randomUUID().toString());
    odpLocators.sapClient.sendKeys(CDAPUtils.getPluginProp(client));
    odpLocators.loadServer.click();
    odpLocators.msHost.sendKeys(CDAPUtils.getPluginProp(asHost));
    odpLocators.portNumber.sendKeys(CDAPUtils.getPluginProp(msServ));
    odpLocators.systemID.sendKeys(CDAPUtils.getPluginProp(systemID));
    odpLocators.logonGroup.sendKeys(CDAPUtils.getPluginProp(lgrp));
    odpLocators.dataSourceName.sendKeys(CDAPUtils.getPluginProp(dsName));

    js.executeScript("window.scrollBy(0,350)", StringUtils.EMPTY);
    odpLocators.gcsPath.sendKeys(CDAPUtils.getPluginProp(gcsPath));
    odpLocators.splitRow.sendKeys(CDAPUtils.getPluginProp(splitrow));
    odpLocators.packageSize.sendKeys(CDAPUtils.getPluginProp(packageSize));
  }

  public static void enterUserNamePassword(String username, String password) throws IOException {
    odpLocators.usernameCredentials.sendKeys(username);
    odpLocators.passwordCredentials.sendKeys(password);
  }

  public static void selectSync() {
    odpLocators.syncRadio.click();
  }

  public static void getSchema() {
    odpLocators.getSchemaButton.click();
  }

  public static void closeButton() {
    odpLocators.closeButton.click();
  }
}
