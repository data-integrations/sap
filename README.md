# SAP
Collection of SAP pluginis

### E2E automation framework
For e2e automation execution of SAP plugins, maven profile e2e-tests have been configured. Important note ragrding it:
1) For it to work, github actions have to be configured (not a special action, just run a below mentioned shell command):

   `/snap/bin/gsutil cp gs://jcoconnector-sapautomation/*.jar ${project.basedir}/src/e2e-test/java/io/cdap/plugin/odp/lib`