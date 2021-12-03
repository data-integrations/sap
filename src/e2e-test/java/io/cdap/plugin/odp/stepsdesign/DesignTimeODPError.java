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

import io.cdap.e2e.utils.SeleniumDriver;
import io.cdap.plugin.odp.locators.ODPLocators;
import io.cdap.plugin.odp.utils.CDAPUtils;
import io.cucumber.java.en.Then;
import org.junit.Assert;
import org.openqa.selenium.By;
import org.openqa.selenium.Keys;
import org.openqa.selenium.WebElement;
import stepsdesign.BeforeActions;

/**
 * DesignTimeODPError.
 */
public class DesignTimeODPError {
  static boolean errorExist = false;
  static String color;

  @Then("{string} as {string} and getting {string}")
  public void userIsAbleToSetParameterAsAndGettingErrorMessage(String configParam, String input, String errorMessage) {
    WebElement elementIn = SeleniumDriver.getDriver().findElement(
      By.xpath("//*[@data-cy='" + configParam + "' and @class='MuiInputBase-input']"));
    CDAPUtils.clearField(elementIn);
    ODPLocators.validateButton.click();
    color = ODPLocators.rowError.getCssValue("border-color");
    errorExist = CDAPUtils.getErrorProp(errorMessage).toLowerCase().contains(ODPLocators.rowError.getText()
                                                                               .toLowerCase());
    Assert.assertTrue(errorExist);
  }

  @Then("User is able to set parameter {string} as {string} and getting {string} for wrong input")
  public void userIsAbleToSetParameterAsAndGettingForWrongInput(String element, String input, String errorMessage) {
    errorExist = false;
    WebElement elementIn = SeleniumDriver.getDriver().findElement(
      By.xpath("//*[@data-cy='" + element + "' and @class='MuiInputBase-input']"));
    CDAPUtils.clearField(elementIn);
    elementIn.sendKeys(input);
    ODPLocators.validateButton.click();
    errorExist = CDAPUtils.getErrorProp(errorMessage).toLowerCase().contains(ODPLocators.jcoError.getText()
                                                                               .toLowerCase());
    Assert.assertTrue(errorExist);
  }

  @Then("User is able to set parameter {string} as {string} and getting {string} for wrong input of password")
  public void userIsAbleToSetParameterAsAndGettingForWrongInputOfPassword(String element, String input1,
                                                                          String errorMessage) {
    errorExist = false;
    WebElement elementPass = SeleniumDriver.getDriver().findElement(
      By.xpath("//*[@data-cy='" + element + "' and @type='password']"));
    CDAPUtils.clearField(elementPass);
    ODPLocators.validateButton.click();
    errorExist = CDAPUtils.getErrorProp(errorMessage).toLowerCase().contains(ODPLocators.jcoError.getText()
                                                                               .toLowerCase());
    color = ODPLocators.rowError.getCssValue("border-color");
    Assert.assertTrue(errorExist);
  }

  @Then("^User is able to set parameter (.+) as (.+) and getting row (.+) for wrong input$")
  public void userIsAbleToSetParameterAsAndGettingRowForWrongInput(
    String option, String input, String errorMessage) {
    errorExist = false;
    WebElement element = SeleniumDriver.getDriver().findElement(
      By.xpath("//*[@data-cy='" + option + "' and @class='MuiInputBase-input']"));
    CDAPUtils.clearField(element);
    SeleniumDriver.getDriver().findElement(By.xpath("//*[@data-cy='" + option + "' and @class='MuiInputBase-input']")).
      sendKeys(input);
    ODPLocators.validateButton.click();
    errorExist = CDAPUtils.getErrorProp(errorMessage).toLowerCase().contains(ODPLocators.rowError.getText()
                                                                               .toLowerCase());
    color = ODPLocators.rowError.getCssValue("border-color");
    Assert.assertTrue(errorExist);
    BeforeActions.scenario.write("Color of the text box" + color);
    Assert.assertTrue(color.toLowerCase().contains("rgb(164, 4, 3)"));
  }

  @Then("^User is able to set parameters filterEqualKey as (.+) and its filterEqualVal " +
    "as (.+) and getting row (.+) for wrong input$")
  public void userIsAbleToSetParametersFilterequalkeyAsAndItsFilterequalvalAsAndGettingError
    (String filterOption, String query, String errorMessage) throws Throwable {
    errorExist = false;
    ODPLocators.filterEqualKey.sendKeys(filterOption);
    ODPLocators.filterEqualVal.sendKeys(query);
    ODPLocators.validateButton.click();
    errorExist = ODPLocators.rowError.getText().toLowerCase().contains(CDAPUtils.getErrorProp(errorMessage)
                                                                         .toLowerCase());
    color = ODPLocators.rowError.getCssValue("border-color");
    Assert.assertTrue(errorExist);
  }

  @Then("User is able to validate the text box is highlighted")
  public void userIsAbleToValidateTheTextBoxIsHighlighted() {
    BeforeActions.scenario.write("Color of the text box" + color);
    Assert.assertTrue(color.toLowerCase().contains("rgb(164, 4, 3)"));
  }

  @Then("^User is able to set parameters filterRangeKey as (.+) and its filterRangeVal " +
    "as (.+) and getting row (.+) for wrong input$")
  public void filterRangeKeyAsFilterOptionAndItsFilterRangeValAsQueryAndGettingRowErrorMessageForWrongInput
    (String filterOption, String query, String errorMessage) throws Throwable {
    ODPLocators.filterRangeKey.sendKeys(filterOption);
    ODPLocators.filterRangeVal.sendKeys(query);
    ODPLocators.validateButton.click();
  }
}
