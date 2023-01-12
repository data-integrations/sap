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
Feature: Design Time ODP Scenario

  @ODP @DesignTime-TC-ODP-DSGN-01(Direct)
  Scenario:User configured direct connection parameters and Security parameters by providing values on SAP UI(ENV)
    Given Open CDF application to configure pipeline
    When Source is SAP ODP
    When Configure Direct Connection "s4Client" "s4Sysnr" "s4AsHost" "dsMasterAttr" "s4GcsPath" "s4Splitrow" "s4PkgSize"
    When Username and Password is provided
    Then Connection is established

  @ODP @DesignTime-TC-ODP-DSGN-01(LOAD)
  Scenario:User configured Load connection parameters and Security parameters by providing values on SAP UI(ENV)
    Given Open CDF application to configure pipeline
    When Source is SAP ODP
    When LoadProp "s4Client" "s4AsHost" "s4MsServ" "s4SystemId" "dsAllDataType" "s4GcsPath" "s4Splitrow" "s4PkgSize" "s4Lgrp"
    When Username and Password is provided
    Then Connection is established

  @ODP @DesignTime-TC-ODP-DSGN-01.02
  Scenario: User is able to configure Security parameters using macros in direct connection
    Given Open CDF application to configure pipeline
    When Source is SAP ODP
    When Configure Direct Connection "s4Client" "s4Sysnr" "s4AsHost" "dsMasterAttr" "s4GcsPath" "s4Splitrow" "s4PkgSize"
    When Username and Password is provided
    When User has selected Sap client macro to configure
    Then User is validate without any error
    When User has selected Sap language macro to configure
    Then User is validate without any error
    When User has selected Sap server as host macro to configure
    Then User is validate without any error
    When User has selected System Number macro to configure
    Then User is validate without any error
    When User has selected datasource macro to configure
    Then User is validate without any error
    When User has selected gcsPath macro to configure
    Then User is validate without any error

  @ODP @DesignTime-TC-ODP-DSGN-01.05
  Scenario: User is able to configure Security parameters using macros in load connection
    Given Open CDF application to configure pipeline
    When Source is SAP ODP
    When LoadProp "s4Client" "s4AsHost" "s4MsServ" "s4SystemId" "dsAllDataType" "s4GcsPath" "s4Splitrow" "s4PkgSize" "s4Lgrp"
    When Username and Password is provided
    When User has selected Sap msHost macro to configure
    Then User is validate without any error
    When User has selected Sap msServ macro to configure
    Then User is validate without any error
    When User has selected UserName and Password macro to configure
    Then User is validate without any error

  @ODP @DesignTime-TC-ODP-DSGN-05.01
  Scenario:User is able to get the schema of the datasources supporting all the datatype
    Given Open CDF application to configure pipeline
    When Source is SAP ODP
    When Configure Direct Connection "s4Client" "s4Sysnr" "s4AsHost" "dsAllDataType" "s4GcsPath" "s4Splitrow" "s4PkgSize"
    When Username and Password is provided
    Then Validate the Schema created


  @ODP @DesignTime-TC-ODP-DSGN-09.01
  Scenario Outline: User is able to get the schema of the SAP Datasource
    Given Open CDF application to configure pipeline
    When Source is SAP ODP
    When Configure Direct Connection "s4Client" "s4Sysnr" "s4AsHost" "dsAllDataType" "s4GcsPath" "s4Splitrow" "s4PkgSize"
    When Username and Password is provided
    When data source as "<datasource>" is added
    Then Validate the Schema created
    Examples:
      | datasource          |
      | 2LIS_02_ITM         |
      | 2LIS_11_VAITM       |
      | 0MATERIAL_LPRH_HIER |
