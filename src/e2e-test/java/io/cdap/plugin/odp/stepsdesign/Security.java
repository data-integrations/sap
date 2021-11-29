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
    ODPActions.clickAllMacroElements();
    CDAPUtils.clearField(ODPLocators.usernameCredentials);
    CDAPUtils.clearField(ODPLocators.macroPass);
    CDAPUtils.clearField(ODPLocators.sapClient);
    CDAPUtils.clearField(ODPLocators.systemNumber);
    CDAPUtils.clearField(ODPLocators.sapApplicationServerHost);
    CDAPUtils.clearField(ODPLocators.dataSourceName);
    CDAPUtils.clearField(ODPLocators.packageSize);
    CDAPUtils.clearField(ODPLocators.splitRow);
    CDAPUtils.clearField(ODPLocators.gcsPath);
    CDAPUtils.clearField(ODPLocators.projectID);
    CDAPUtils.clearField(ODPLocators.language);
    CDAPUtils.clearField(ODPLocators.extractType);

    ODPActions.clickMacroElement(4);
    ODPActions.clickMacroElement(10);
    ODPActions.clickMacroElement(11);
    ODPActions.clickMacroElement(12);
    ODPLocators.sapClient.sendKeys("${clientmacro}");
    ODPLocators.language.sendKeys("${languagemacro}");
    ODPLocators.sapApplicationServerHost.sendKeys("${serverhostmacro}");
    ODPLocators.systemNumber.sendKeys("${sysnrmacro}");
    ODPLocators.dataSourceName.sendKeys("${datasourcemacro}");
    ODPLocators.extractType.sendKeys("${loadtype}");
    ODPLocators.usernameCredentials.sendKeys("${usermacro}");
    ODPLocators.macroPass.sendKeys("${passmacro}");
    ODPLocators.projectID.sendKeys("${gcpprojectid}");
    ODPLocators.gcsPath.sendKeys("${gcsbucket}");
    ODPLocators.splitRow.sendKeys("${numbersplit}");
    ODPLocators.packageSize.sendKeys("${packagesize}");
  }

  @Then("enter the secured created variable")
  public void enterTheSecuredCreatedVariable() throws InterruptedException {
    ODPActions.clickAllMacroElements();
    CDAPUtils.clearField(ODPLocators.sapClient);
    CDAPUtils.clearField(ODPLocators.language);
    CDAPUtils.clearField(ODPLocators.sapApplicationServerHost);
    CDAPUtils.clearField(ODPLocators.systemNumber);
    CDAPUtils.clearField(ODPLocators.sapRouter);
    CDAPUtils.clearField(ODPLocators.dataSourceName);
    CDAPUtils.clearField(ODPLocators.extractType);
    CDAPUtils.clearField(ODPLocators.usernameCredentials);
    CDAPUtils.clearField(ODPLocators.macroPass);
    CDAPUtils.clearField(ODPLocators.gcsProjectID);
    CDAPUtils.clearField(ODPLocators.packageSize);
    CDAPUtils.clearField(ODPLocators.splitRow);
    CDAPUtils.clearField(ODPLocators.gcsPath);
    CDAPUtils.clearField(ODPLocators.subsName);
    CDAPUtils.clearField(ODPLocators.filterEqualMacros);
    CDAPUtils.clearField(ODPLocators.filterEqualMacros);

    ODPActions.clickMacroElement(4);
    ODPActions.clickMacroElement(10);
    ODPActions.clickMacroElement(11);
    ODPActions.clickMacroElement(12);
    ODPActions.clickMacroElement(13);
    ODPLocators.sapClient.sendKeys("${secure(testjcoclient)}");
    ODPLocators.language.sendKeys("${secure(testlang)}");
    ODPLocators.sapApplicationServerHost.sendKeys("${secure(testjcoserver)}");
    ODPLocators.systemNumber.sendKeys("${secure(testjcosysnr)}");
    ODPLocators.dataSourceName.sendKeys("${secure(testjcodatasourcename)}");
    ODPLocators.extractType.sendKeys("${secure(testloadtype)}");
    ODPLocators.usernameCredentials.sendKeys("${secure(testuserqa)}");
    ODPLocators.macroPass.sendKeys("${secure(testpasswordqa)}");
    ODPLocators.gcsProjectID.sendKeys("auto-detect");
    ODPLocators.gcsPath.sendKeys("${secure(testgcspath)}");
    ODPLocators.splitRow.sendKeys("${secure(testjcosplit)}");
    ODPLocators.packageSize.sendKeys("${secure(testjcopackagesize)}");
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

  @Then("User is able to validate the validate the error no auth error")
  public void userIsAbleToValidateTheValidateTheErrorNoAuthError() {
      Assert.assertTrue(errorExist);
  }
}
