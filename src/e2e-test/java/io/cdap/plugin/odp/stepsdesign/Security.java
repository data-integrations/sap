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

import io.cdap.e2e.pages.actions.CdfSysAdminActions;
import io.cdap.e2e.pages.locators.CdfStudioLocators;
import io.cdap.e2e.pages.locators.CdfSysAdminLocators;
import io.cdap.e2e.utils.CdfHelper;
import io.cdap.e2e.utils.SeleniumDriver;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.odp.actions.ODPActions;
import io.cdap.plugin.odp.locators.ODPLocators;
import io.cdap.plugin.odp.utils.CDAPUtils;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.junit.Assert;
import org.openqa.selenium.NoAlertPresentException;

import java.io.IOException;

/**
 * Security.
 */
public class Security implements CdfHelper {

  String arr[] = new String[11];
  private static int countarr = 0;
  private static boolean errorExist;

  @Given("Open {string} link to configure macros")
  public void openLinkToConfigureMacros(String link) throws IOException {
    SeleniumDriver.getDriver().get(CDAPUtils.getPluginProp(link));
    SeleniumDriver.waitForPageToLoad();
    try {
      SeleniumDriver.getDriver().switchTo().alert().accept();
      SeleniumDriver.waitForPageToLoad();
    } catch (NoAlertPresentException var3) {
      SeleniumDriver.waitForPageToLoad();
    }
  }

  @Then("Select {string} service to configure")
  public void selectServiceToConfigure(String service) {
    CdfSysAdminActions.selectMacroAPIService(service);
  }

  @Then("enter variable for {string} of the macro")
  public void enterVariableForOfTheMacro(String arg0) {
    countarr = arg0.length();
    CDAPUtils.clearField(CdfSysAdminLocators.apiInputURI);
    CdfSysAdminActions.enterURI("namespaces/default/securekeys/" + arg0);
  }

  @Then("enter the {string} of the service")
  public void enterTheOfTheService(String request) throws IOException {
    CDAPUtils.clearField(CdfSysAdminLocators.requestBody);
    CdfSysAdminActions.enterRequestBody(CDAPUtils.getPluginProp(request));
  }

  @Then("send request and verify success message")
  public void sendRequestAndVerifySuccessMessage() throws IOException {
    CdfSysAdminActions.clearRequest();
    CdfSysAdminActions.clearAllRequest();
    CdfSysAdminActions.sendRequest();
    CdfSysAdminActions.verifySuccess();
  }

  @Then("Link Source and Sink table")
  public void linkSourceAndSinkTable() throws InterruptedException {
    SeleniumHelper.dragAndDrop(CdfStudioLocators.fromSAPODP, CdfStudioLocators.toBigQiery);
  }

  @Then("enter the macro variable in fields")
  public void enterTheMacroVariableInFields() throws InterruptedException {
    ODPLocators.macroSapUserName.click();
    ODPLocators.macroSapUserPazwrd.click();
    ODPLocators.macroSapClient.click();
    ODPLocators.macroSapSysNumber.click();
    ODPLocators.macroSapASHost.click();
    ODPLocators.macroSapDsName.click();
    ODPLocators.macroPackSize.click();
    ODPLocators.macroSplits.click();
    ODPLocators.macroSapGcsPath.click();
    ODPLocators.macroSapGcsProjId.click();
    ODPLocators.macroSapLanguage.click();
    ODPLocators.macroSapExtractType.click();
    SeleniumHelper.replaceElementValue(ODPLocators.usernameCredentials, "${usermacro}");
    SeleniumHelper.replaceElementValue(ODPLocators.macroPass, "${passmacro}");
    SeleniumHelper.replaceElementValue(ODPLocators.sapClient, "${clientmacro}");
    SeleniumHelper.replaceElementValue(ODPLocators.systemNumber, "${sysnrmacro}");
    SeleniumHelper.replaceElementValue(ODPLocators.sapApplicationServerHost, "${serverhostmacro}");
    SeleniumHelper.replaceElementValue(ODPLocators.dataSourceName, "${datasourcemacro}");
    SeleniumHelper.replaceElementValue(ODPLocators.packageSize, "${packagesize}");
    SeleniumHelper.replaceElementValue(ODPLocators.splitRow, "${numbersplit}");
    SeleniumHelper.replaceElementValue(ODPLocators.gcsPath, "${gcsbucket}");
    SeleniumHelper.replaceElementValue(ODPLocators.projectID, "${gcpprojectid}");
    SeleniumHelper.replaceElementValue(ODPLocators.language, "${languagemacro}");
    SeleniumHelper.replaceElementValue(ODPLocators.extractType, "${loadtype}");
  }

  @Then("enter the secured created variable")
  public void enterTheSecuredCreatedVariable() throws InterruptedException {
    ODPLocators.macroSapUserName.click();
    ODPLocators.macroSapUserPazwrd.click();
    ODPLocators.macroSapClient.click();
    ODPLocators.macroSapSysNumber.click();
    ODPLocators.macroSapASHost.click();
    ODPLocators.macroSapDsName.click();
    ODPLocators.macroPackSize.click();
    ODPLocators.macroSplits.click();
    ODPLocators.macroSapGcsPath.click();
    ODPLocators.macroSapGcsProjId.click();
    ODPLocators.macroSapLanguage.click();
    ODPLocators.macroSapExtractType.click();
    CDAPUtils.clearField(ODPLocators.sapRouter);
    CDAPUtils.clearField(ODPLocators.subsName);
    CDAPUtils.clearField(ODPLocators.filterEqualMacros);
    SeleniumHelper.replaceElementValue(ODPLocators.sapClient, "${secure(testjcoclient)}");
    SeleniumHelper.replaceElementValue(ODPLocators.language, "${secure(testlang)}");
    SeleniumHelper.replaceElementValue(ODPLocators.sapApplicationServerHost, "${secure(testjcoserver)}");
    SeleniumHelper.replaceElementValue(ODPLocators.systemNumber, "${secure(testjcosysnr)}");
    SeleniumHelper.replaceElementValue(ODPLocators.dataSourceName, "${secure(testjcodatasourcename)}");
    SeleniumHelper.replaceElementValue(ODPLocators.extractType, "${secure(testloadtype)}");
    SeleniumHelper.replaceElementValue(ODPLocators.usernameCredentials, "${secure(testuserqa)}");
    SeleniumHelper.replaceElementValue(ODPLocators.macroPass, "${secure(testpasswordqa)}");
    SeleniumHelper.replaceElementValue(ODPLocators.gcsProjectID, "auto-detect");
    SeleniumHelper.replaceElementValue(ODPLocators.packageSize, "${secure(testjcopackagesize)}");
    SeleniumHelper.replaceElementValue(ODPLocators.splitRow, "${secure(testjcosplit)}");
    SeleniumHelper.replaceElementValue(ODPLocators.gcsPath, "${secure(testgcspath)}");
    ODPLocators.validateButton.click();
    ODPLocators.pluginValidationSuccessMessage.isDisplayed();
  }

  @Then("enter the {string} of the service username and password")
  public void enterTheOfTheServiceUsernameAndPassword(String request) throws IOException {
    try {
      CDAPUtils.clearField(CdfSysAdminLocators.requestBody);
    } finally {
      CDAPUtils.clearField(CdfSysAdminLocators.requestBody);
    }
    CdfSysAdminActions.enterRequestBody("{\"description\": \"secure login creds\",\"data\": \"" + CDAPUtils.
      getPluginProp(request) + "\",\"properties\": {}}");
  }

  @When("Username {string} and Password {string} is provided")
  public void usernameAndPasswordIsProvided(String userName, String password) throws IOException {
    ODPActions.enterUserNamePassword(CDAPUtils.getPluginProp(userName), CDAPUtils.getPluginProp(password));
  }

  @Then("RFC auth error is displayed {string}")
  public void rfcAuthErrorIsDisplayed(String rfcError) {
    ODPActions.getSchema();
    errorExist = ODPLocators.mainStreamError.getText().toLowerCase().contains(CDAPUtils.getErrorProp(rfcError)
            .toLowerCase());
  }

  @Then("User is able to validate the error no auth error")
  public void userIsAbleToValidateTheValidateTheErrorNoAuthError() {
      Assert.assertTrue(errorExist);
  }
}
