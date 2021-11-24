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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.adapter.connector.SAPAdapterImpl;
import com.google.adapter.connector.SAPProperties;
import com.google.adapter.exceptions.SystemException;
import com.google.adapter.util.ErrorCapture;
import com.google.adapter.util.ExceptionUtils;
import com.sap.conn.jco.JCoException;
import io.cdap.e2e.pages.actions.CdfBigQueryPropertiesActions;
import io.cdap.e2e.pages.actions.CdfGcsActions;
import io.cdap.e2e.pages.actions.CdfLogActions;
import io.cdap.e2e.pages.actions.CdfPipelineRunAction;
import io.cdap.e2e.pages.actions.CdfStudioActions;
import io.cdap.e2e.pages.locators.CdfBigQueryPropertiesLocators;
import io.cdap.e2e.pages.locators.CdfPipelineRunLocators;
import io.cdap.e2e.pages.locators.CdfStudioLocators;
import io.cdap.e2e.utils.CdfHelper;
import io.cdap.e2e.utils.GcpClient;
import io.cdap.e2e.utils.SeleniumDriver;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.odp.actions.ODPActions;
import io.cdap.plugin.odp.locators.ODPLocators;
import io.cdap.plugin.odp.utils.CDAPUtils;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.openqa.selenium.By;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;
import stepsdesign.BeforeActions;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * RuntimeODP.
 */
public class RuntimeODP implements CdfHelper {
  private static final Logger logger = Logger.getLogger(RuntimeODP.class);
  GcpClient gcpClient = new GcpClient();
  private SAPProperties sapProps;
  private ErrorCapture errorCapture;
  private SAPAdapterImpl sapAdapterImpl;
  private ExceptionUtils exceptionUtils;
  static int dsRecordsCount;
  static int countRecords;
  static int presentRecords;
  static int counter = 0;
  static String number;
  static int noOfRecords;
  static String deltaLog;
  static String rawLog;
  static PrintWriter out;
  private List<String> numList = new ArrayList<>();
  public static CdfPipelineRunLocators cdfPipelineRunLocators =
    SeleniumHelper.getPropertiesLocators(CdfPipelineRunLocators.class);

  static {
    try {
      out = new PrintWriter(BeforeActions.myObj);
    } catch (FileNotFoundException e) {
      logger.error("Failed while printWriter : " + e);
    }
  }

  public RuntimeODP() throws FileNotFoundException {
    number = RandomStringUtils.randomAlphanumeric(7);
  }

  @Given("Open CDF application to configure pipeline")
  public void openCDFApplicationToConfigurePipeline() throws IOException, InterruptedException {
    openCdf();
  }

  @When("Source is SAP ODP")
  public void sourceIsSAPODP() {
    ODPActions.selectODPSource();
  }

  @When("Target is BigQuery for ODP data transfer")
  public void targetIsBigQueryForODPDataTransfer() {
    CdfStudioActions.sinkBigQuery();
  }

  @When("Configure Direct Connection {string} {string} {string} {string} {string} {string} {string}")
  public void configureDirectConnection(String client, String sysnr, String asHost, String dsName, String gcsPath,
                                        String splitRow, String packageSize) throws IOException, InterruptedException {
    ODPActions.getODPProperties();
    ODPActions.enterDirectConnectionProperties(client, sysnr, asHost, dsName, gcsPath, splitRow, packageSize);
  }

  @When(
    "Configure Connection {string} {string} {string} {string} {string} {string} {string} {string} {string} {string}")
  public void configureConnection(String client, String sysnr, String asHost, String dsName, String gcsPath,
                                  String splitRow, String packageSize, String msServ, String systemID,
                                  String lgrp) throws IOException, InterruptedException {
    ODPActions.getODPProperties();
    ODPActions.enterConnectionProperties(client, sysnr, asHost, dsName, gcsPath, splitRow, packageSize,
                                         msServ, systemID, lgrp);
  }

  @When("Username and Password is provided")
  public void usernameAndPasswordIsProvided() throws IOException {
    ODPActions.enterUserNamePassword(CDAPUtils.getPluginProp("AUTH_USERNAME"),
                                     CDAPUtils.getPluginProp("AUTH_PASSWORD"));
  }

  @When("Run one Mode is Sync mode")
  public void runOneModeIsSyncMode() {
    ODPActions.selectSync();
  }

  @Then("Validate the Schema created")
  public void validateTheSchemaCreated() throws InterruptedException {
    ODPActions.getSchema();
    WebDriverWait wait = new WebDriverWait(SeleniumDriver.getDriver(), 20000);
    wait.until(ExpectedConditions.numberOfElementsToBeMoreThan
      (By.xpath("//*[@placeholder=\"Field name\"]"), 2));
  }

  @Then("Close the ODP Properties")
  public void closeTheODPProperties() {
    ODPActions.closeButton();
  }

  @Then("Delete the existing Odp table {string} in bigquery")
  public void deleteTheExistingOdpTableInBigquery(String table) throws IOException, InterruptedException {
    gcpClient.dropBqQuery(CDAPUtils.getPluginProp(table));
    BeforeActions.scenario.write("Table Deleted Successfully");
  }

  @Then("link source and target")
  public void linkSourceAndTarget() {
    SeleniumHelper.dragAndDrop(ODPLocators.fromODP, CdfStudioLocators.toBigQiery);
  }

  @Then("Delete all existing records in datasource")
  public void deleteAllExistingRecordsInDatasource() throws JCoException, IOException {
    Properties connection = readPropertyODP();
    sapProps = SAPProperties.getDefault(connection);
    errorCapture = new ErrorCapture(exceptionUtils);
    sapAdapterImpl = new SAPAdapterImpl(errorCapture, connection);
    Map opProps = new HashMap<>();
    opProps.put("RFC", "ZTEST_DATA_DEL_ACDOCA");
    opProps.put("autoCommit", "true");
    try {
      ObjectMapper mapper = new ObjectMapper();
      ObjectNode objectNode = mapper.createObjectNode();
      objectNode.put("I_ALL", "X");
      JsonNode response = sapAdapterImpl.executeRFC(objectNode.toString(), opProps,
                                                    StringUtils.EMPTY, StringUtils.EMPTY);
    } catch (Exception e) {
      throw SystemException.throwException(e.getMessage(), e);
    }
  }

  public Properties readPropertyODP() throws IOException {
    Properties prop = new Properties();
    InputStream input;
    Properties connection = new SAPProperties();
    input = new FileInputStream("src/e2e-test/resources/Google_SAP_Connection.properties");
    prop.load(input);
    Set<String> propertyNames = prop.stringPropertyNames();
    for (String property : propertyNames) {
      connection.put(property, prop.getProperty(property));
    }
    return connection;
  }

  public static void main(String args[]) throws IOException, JCoException, InterruptedException {
    RuntimeODP runtimeODP = new RuntimeODP();
    runtimeODP.deleteTheExistingOdpTableInBigquery("tab_src01");
  }

  @Then("{string} the {string} records with {string} in the ODP datasource from JCO")
  public void createTheRecordsInTheODPDatasourceFromJCO(String action, String recordcount, String rfcName)
    throws IOException, JCoException {
    if (action.equalsIgnoreCase("create")) {
      action = "I_NUM_C";
    } else if (action.equalsIgnoreCase("delete")) {
      action = "I_NUM_D";
    } else if (action.equalsIgnoreCase("update")) {
      action = "I_NUM_U";
    }
    dsRecordsCount = Integer.parseInt(recordcount);
    Properties connection = readPropertyODP();
    sapProps = SAPProperties.getDefault(connection);
    errorCapture = new ErrorCapture(exceptionUtils);
    sapAdapterImpl = new SAPAdapterImpl(errorCapture, connection);
    Map opProps = new HashMap<>();
    opProps.put("RFC", CDAPUtils.getPluginProp(rfcName));
    opProps.put("autoCommit", "true");
    try {

      ObjectMapper mapper = new ObjectMapper();
      ObjectNode objectNode = mapper.createObjectNode();
      objectNode.put(action, recordcount);
      JsonNode response = sapAdapterImpl.executeRFC(objectNode.toString(), opProps, "", "");
      noOfRecords = Integer.parseInt(response.get("EX_COUNT").asText());
      Iterator<JsonNode> iteratedData = response.get("EX_DATA").iterator();
      while (iteratedData.hasNext()) {
        JsonNode object = iteratedData.next();
        int zeroIndex = 0;
        Iterator<String> fieldName = object.fieldNames();
        while (fieldName.hasNext() && zeroIndex == 0) {
          numList.add(object.get(fieldName.next()).asText());
          zeroIndex++;
        }
      }
      BeforeActions.scenario.write("No of records :-" + noOfRecords + "/nArrays.toString(numList.toArray())");
      Assert.assertEquals(noOfRecords, Integer.parseInt(recordcount));
      Thread.sleep(60000);
    } catch (Exception e) {
      throw SystemException.throwException(e.getMessage(), e);
    }
  }

  @Then("Enter the BigQuery Properties for ODP datasource {string}")
  public void enterTheBigQueryPropertiesForODPDatasource(String tableName) throws IOException, InterruptedException {
    CdfStudioLocators.bigQueryProperties.click();
    CdfBigQueryPropertiesActions cdfBigQueryPropertiesActions = new CdfBigQueryPropertiesActions();
    SeleniumHelper.replaceElementValue(CdfBigQueryPropertiesLocators.projectID,
                                       CDAPUtils.getPluginProp("odpProjectId"));
    CdfBigQueryPropertiesLocators.bigQueryReferenceName.sendKeys("automation_test");
    CdfBigQueryPropertiesLocators.dataSetProjectID.sendKeys(CDAPUtils.getPluginProp("odpProjectId"));
    CdfBigQueryPropertiesLocators.dataSet.sendKeys(CDAPUtils.getPluginProp("dataSetOdp"));
    CdfBigQueryPropertiesLocators.bigQueryTable.sendKeys(CDAPUtils.getPluginProp(tableName));
    CdfBigQueryPropertiesLocators.truncatableSwitch.click();
    CdfBigQueryPropertiesLocators.updateTable.click();
    CdfBigQueryPropertiesLocators.validateBttn.click();
    SeleniumHelper.waitElementIsVisible(CdfBigQueryPropertiesLocators.textSuccess, 1L);
  }

  @Then("Close the BQ Properties")
  public void closeTheBQProperties() {
    CdfGcsActions.closeButton();
  }

  @Then("Run the ODP Pipeline in Runtime")
  public void runTheODPPipelineInRuntime() throws InterruptedException {
    CdfPipelineRunAction.runClick();
  }

  @Then("Wait till ODP pipeline is in running state")
  public void waitTillODPPipelineIsInRunningState() {
    WebDriverWait wait = new WebDriverWait(SeleniumDriver.getDriver(), 1000000);
    wait.until(ExpectedConditions.or(
      ExpectedConditions.visibilityOfElementLocated(By.xpath("//*[@data-cy='Succeeded']")),
      ExpectedConditions.visibilityOfElementLocated(By.xpath("//*[@data-cy='Failed']"))));
  }

  @Then("Open Logs of ODP Pipeline")
  public void openLogsOfODPPipeline() throws InterruptedException, FileNotFoundException {
    CdfPipelineRunAction.logsClick();
    Thread.sleep(5000); //TODO
    rawLog = CdfPipelineRunAction.captureRawLogs();
    BeforeActions.scenario.write(rawLog);
    out.println(rawLog);
    out.close();
  }

  @Then("Verify the ODP pipeline status is {string}")
  public void verifyTheODPPipelineStatusIs(String status) {
    boolean webelement = SeleniumHelper.verifyElementPresent("//*[@data-cy='" + status + "']");
    Assert.assertTrue(webelement);
  }

  @Then("validate successMessage is displayed for the ODP pipeline")
  public void validateSuccessMessageIsDisplayedForTheODPPipeline() {
    CdfLogActions.validateSucceeded();
  }

  @Then("Get Count of no of records transferred from ODP to BigQuery in {string}")
  public void getCountOfNoOfRecordsTransferredFromODPToBigQueryIn(String table) throws
    IOException, InterruptedException {
    countRecords = gcpClient.countBqQuery(CDAPUtils.getPluginProp(table));
  }

  @Then("Verify the Delta load transfer is successful in {string} on basis of {string}")
  public void verifyTheDeltaLoadTransferIsSuccessfull(String table, String field) throws IOException,
    InterruptedException {
    int i = 0;
    String projectId = CDAPUtils.getPluginProp("odpProjectId");
    String datasetName = CDAPUtils.getPluginProp("dataSetOdp");
    String selectQuery = "SELECT count(*)  FROM `" + projectId + "." + datasetName + "." + CDAPUtils.getPluginProp
      (table) + "` WHERE " +
      field.toUpperCase();
    for (i = 0; i < numList.size(); i++) {
      Assert.assertTrue(GcpClient.executeQuery(selectQuery.concat("=" + "\"" + numList.get(i) + "\"")) == 1);
    }
    countRecords = gcpClient.countBqQuery(CDAPUtils.getPluginProp(table));
    Assert.assertTrue(countRecords == noOfRecords);
  }

  @Then("Close the log window")
  public void closeTheLogWindow() {
    SeleniumDriver.getDriver().findElement(By.xpath("//*[@data-cy=\"log-viewer-close-btn\"]")).click();
    numList.clear();
  }

  @Then("delete table {string} in BQ if not empty")
  public void deleteTableInBQIfNotEmpty(String table) throws IOException, InterruptedException {
    try {
      int existingRecords = gcpClient.countBqQuery(CDAPUtils.getPluginProp(table));
      if (existingRecords > 0) {
        gcpClient.dropBqQuery(CDAPUtils.getPluginProp(table));
        BeforeActions.scenario.write("Table Deleted Successfully");
      }
    } catch (Exception e) {
      BeforeActions.scenario.write(e.toString());
    }
  }

  @Then("Wait till ODP pipeline is successful again")
  public void waitTillODPPipelineIsSuccessfulAgain() {
    SeleniumHelper.isElementPresent(SeleniumDriver.getDriver().findElement(By.xpath("//*[@data-cy='Running']")));
    WebDriverWait wait = new WebDriverWait(SeleniumDriver.getDriver(), 1000000);
    SeleniumDriver.getDriver().get(SeleniumDriver.getDriver().getCurrentUrl());
    wait.until(ExpectedConditions.or(
      ExpectedConditions.visibilityOfElementLocated(By.xpath("//*[@data-cy='Succeeded']")),
      ExpectedConditions.visibilityOfElementLocated(By.xpath("//*[@data-cy='Failed']"))));
  }

  @Then("Verify the full load transfer is successful")
  public void verifyTheFullLoadTransferIssuccessful() {
    Assert.assertTrue(countRecords == recordOut());
  }

  @Then("Open Logs of ODP Pipeline to capture delta logs")
  public void openLogsOfODPPipelineToCaptureDeltaLogs() throws InterruptedException, FileNotFoundException {
    PrintWriter out = new PrintWriter(BeforeActions.myObj);
    WebDriverWait wait = new WebDriverWait(SeleniumDriver.getDriver(), 10000);
    wait.until(ExpectedConditions.refreshed(ExpectedConditions.visibilityOfElementLocated(By.
      xpath("//*[@class=\"run-logs-btn\"]"))));
    CdfPipelineRunAction.logsClick();
    wait.until(ExpectedConditions.refreshed(ExpectedConditions.visibilityOfElementLocated(By.
      xpath("(//*[@type=\"button\"])[7]"))));
    deltaLog = CdfPipelineRunAction.captureRawLogs();
    BeforeActions.scenario.write(deltaLog);
    out.println(rawLog.concat("********************+delta" + deltaLog));
    out.close();
  }

  @When("Subscriber is entered")
  public void subscriberIsEntered() {
    ODPLocators.subsName.sendKeys(number);
  }

  @Then("Save and Deploy ODP Pipeline")
  public void saveAndDeployODPPipeline() throws InterruptedException {
    saveAndDeployPipeline();
  }

  @Then("Reset the parameters")
  public void resetTheParameters() {
    countRecords = 0;
    presentRecords = 0;
    dsRecordsCount = 0;
    counter = 0;
    BeforeActions.scenario.write("countRecords=0;\n" +
                                   "    presentRecords=0;\n" +
                                   "    dsRecordsCount=0;");
  }
}
