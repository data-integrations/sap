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
package io.cdap.plugin.odp.stepsdesign;

import io.cdap.e2e.pages.actions.CdfGcsActions;
import io.cdap.e2e.utils.SeleniumDriver;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.odp.actions.ODPActions;
import io.cdap.plugin.odp.locators.ODPLocators;
import io.cdap.plugin.odp.utils.CDAPUtils;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.junit.Assert;
import org.openqa.selenium.By;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;

import java.io.IOException;

/**
 * DesignTimeODP.
 */
public class DesignTimeODP {

  @Then("Connection is established")
  public void connectionIsEstablished() throws InterruptedException {
    CdfGcsActions.getSchema();
    WebDriverWait wait = new WebDriverWait(SeleniumDriver.getDriver(), 20000);
    wait.until(ExpectedConditions.numberOfElementsToBeMoreThan(By.xpath("//*[@placeholder=\"Field name\"]"), 2));
    Assert.assertEquals(true, CDAPUtils.schemaValidation());
  }

  @When("LoadProp {string} {string} {string} {string} {string} {string} {string} {string} {string}")
  public void loadConnection(String client, String asHost, String msServ, String systemID, String dsName,
                             String gcsPath, String splitRow, String pkgSize, String lgrp)
    throws IOException, InterruptedException {
    ODPActions.getODPProperties();
    ODPActions.enterLoadConnectionProperties(
      client, asHost, msServ, systemID, lgrp, dsName, gcsPath, splitRow, pkgSize);
  }

  @When("User has selected Sap client macro to configure")
  public void userHasSelectedSapClientMacroToConfigure() {
    ODPLocators.macroSapClient.click();
    SeleniumHelper.replaceElementValue(ODPLocators.sapClient, "${client}");
  }

  @Then("User is validate without any error")
  public void userIsValidateWithoutAnyError() throws InterruptedException {
    ODPLocators.validateButton.click();
    Assert.assertTrue(ODPLocators.pluginValidationSuccessMessage.isDisplayed());
  }

  @When("User has selected Sap language macro to configure")
  public void userHasSelectedSapLagMacroToConfigure() {
    ODPLocators.macroSapLanguage.click();
    SeleniumHelper.replaceElementValue(ODPLocators.language, "${lang}");
  }

  @When("User has selected Sap server as host macro to configure")
  public void userHasSelectedSapServerAsHostMacroToConfigure() {
    ODPLocators.macroSapASHost.click();
    SeleniumHelper.replaceElementValue(ODPLocators.sapApplicationServerHost, "${host}");
  }

  @When("User has selected System Number macro to configure")
  public void userHasSelectedSystemNumberMacroToConfigure() {
    ODPLocators.macroSapSysNumber.click();
    SeleniumHelper.replaceElementValue(ODPLocators.systemNumber, "${sysnr}");
  }

  @When("User has selected datasource macro to configure")
  public void userHasSelectedDatasourceMacroToConfigure() {
    ODPLocators.macroSapDsName.click();
    SeleniumHelper.replaceElementValue(ODPLocators.dataSourceName, "${dsname}");
  }

  @When("User has selected gcsPath macro to configure")
  public void userHasSelectedGcsPathMacroToConfigure() {
    ODPLocators.macroSapGcsPath.click();
    SeleniumHelper.replaceElementValue(ODPLocators.gcsPath, "${gcs}");
  }

  @When("User has selected Sap msHost macro to configure")
  public void userHasSelectedSapMsHostMacroToConfigure() {
    ODPLocators.macroSapASHost.click();
    SeleniumHelper.replaceElementValue(ODPLocators.msHost, "${msHost}");
  }

  @When("User has selected Sap msServ macro to configure")
  public void userHasSelectedSapMsServMacroToConfigure() {
    ODPLocators.macroSapRouter.click();
    SeleniumHelper.replaceElementValue(ODPLocators.portNumber, "${portNo}");
  }

  @When("User has selected UserName and Password macro to configure")
  public void userHasSelectedUserNameAndPasswordMacroToConfigure() {
    ODPLocators.macroSapGcsPath.click();
    SeleniumHelper.replaceElementValue(ODPLocators.usernameCredentials, "${userName}");
  }

  @When("data source as {string} is added")
  public void dataSourceAsIsAdded(String datasource) throws InterruptedException {
    SeleniumHelper.replaceElementValue(ODPLocators.dataSourceName, datasource);
    ODPLocators.validateButton.click();
    ODPLocators.pluginValidationSuccessMessage.isDisplayed();
  }
}
