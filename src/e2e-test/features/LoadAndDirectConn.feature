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
Feature: Optional Properties

  @LOADnDIR
  Scenario:Load And Direct Connection EventHandler Trial
    Given Open CDF application to configure pipeline
    When Source is SAP ODP
    When Target is BigQuery for ODP data transfer
    When Configure Connection "s4Client" "s4Sysnr" "s4AsHost" "allTypeDsName" "s4GcsPath" "s4Splitrow" "s4PkgSize" "load.S4msServ" "load.S4systemID" "load.S4Lgrp"
    When Username and Password is provided
    Then Validate the Schema created
    Then Close the ODP Properties
    Then delete table "tableDemo" in BQ if not empty
    Then Enter the BigQuery Properties for ODP datasource "tableDemo"
    Then Close the BQ Properties
    Then link source and target
    Then Save and Deploy ODP Pipeline
    Then Run the ODP Pipeline in Runtime
    Then Wait till ODP pipeline is in running state
    Then Open Logs of ODP Pipeline
    Then Verify the ODP pipeline status is "Succeeded"
    Then validate successMessage is displayed for the ODP pipeline
    Then Get Count of no of records transferred from ODP to BigQuery in "tableDemo"
    Then Verify the Delta load transfer is successful in "tableDemo" on basis of "EBELN"  #todo
