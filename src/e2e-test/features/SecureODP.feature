# Copyright © 2021 Cask Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
@Security
Feature: Secured macros can be configured and used for the login credentials

  @ODP @Security-TC-ODP-SECU-01
  Scenario: User is able to configure security macros with auth user and no errors while validating the credentials plus connection parameter
    Given Open "httpCall" link to configure macros
    Then Select "PUT" service to configure
    Then enter variable for "testuserqa" of the macro
    Then enter the "AUTH_USERNAME" of the service username and password
    Then send request and verify success message
    Then enter variable for "testpasswordqa" of the macro
    Then enter the "AUTH_PASSWORD" of the service username and password
    Then send request and verify success message
    Then enter variable for "testjcoclient" of the macro
    Then enter the "macroS4_jco_client" of the service
    Then send request and verify success message
    Then enter variable for "testjcoserver" of the macro
    Then enter the "macroS4_jco_server" of the service
    Then send request and verify success message
    Then enter variable for "testjcosysnr" of the macro
    Then enter the "macroS4_jco_sysnr" of the service
    Then send request and verify success message
    Then enter variable for "testjcodatasourcename" of the macro
    Then enter the "macroS4_jco_DataSourceName" of the service
    Then send request and verify success message
    Then enter variable for "testjcosplit" of the macro
    Then enter the "macroS4_jco_split" of the service
    Then send request and verify success message
    Then enter variable for "testjcopackagesize" of the macro
    Then enter the "macroS4_jco_packageSize" of the service
    Then send request and verify success message
    Then enter variable for "testgcspath" of the macro
    Then enter the "macroS4_jco_gcspath" of the service
    Then send request and verify success message
    Then enter variable for "testlang" of the macro
    Then enter the "macroS4_jco_lang" of the service
    Then send request and verify success message
    Then enter variable for "testloadtype" of the macro
    Then enter the "macroS4_jco_load" of the service
    Then send request and verify success message
    Given Open CDF application to configure pipeline
    When Source is SAP ODP
    When Target is BigQuery for ODP data transfer
    Then link source and target
    When Configure Direct Connection "s4Client" "s4Sysnr" "s4AsHost" "dsHdr" "s4GcsPath" "s4Splitrow" "s4PkgSize"
    Then enter the secured created variable
    Then Close the ODP Properties
    Then Enter the BigQuery Properties for ODP datasource "tableDemo"
    Then Close the BQ Properties
    Then Save and Deploy ODP Pipeline
    Then Run the ODP Pipeline in Runtime
    Then Wait till ODP pipeline is in running state
    Then Open Logs of ODP Pipeline
    Then Verify the ODP pipeline status is "Succeeded"
    Then validate successMessage is displayed for the ODP pipeline
    Then Get Count of no of records transferred from ODP to BigQuery in "tableDemo"
    Then Verify the full load transfer is successful

  @ODP @Security-TS-ODP-SECU-06
  Scenario: Unauthorized user gets an Error message on trying to initiate data transfer  (User dont have access to RFC)
    Given Open CDF application to configure pipeline
    When Source is SAP ODP
    When Target is BigQuery for ODP data transfer
    When Configure Direct Connection "s4Client" "s4Sysnr" "s4AsHost" "dsHdr" "s4GcsPath" "s4Splitrow" "s4PkgSize"
    When Username "testNoAuthUsername" and Password "testNoAuthPwd" is provided
    When Run one Mode is Sync mode
    Then RFC auth error is displayed "rfc_error"
    Then User is able to validate the error no auth error
