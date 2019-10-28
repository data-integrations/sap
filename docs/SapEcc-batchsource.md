# SAP OData Batch Source

Description
-----------
This plugin reads data from SAP OData service.
OData(Open Data Protocol) is an open data access protocol from Microsoft that allows the creation and consumption of
query-able and interoperable RESTful APIs in a simple and standard way. SAP Netweaver Gateway comes with design-time
tools to facilitate modeling OData services for consumption, particularly [SAP Gateway Service Builder]
(transaction SEGW). For detailed information about the end-to-end service development process, see
[Getting Started with the Service Builder].

[SAP Gateway Service Builder]:
https://help.sap.com/doc/saphelp_gateway20sp12/2.0/en-US/1b/c16e1e20a74746ad386bc10b60b6c3/frameset.htm

[Getting Started with the Service Builder]:
https://help.sap.com/doc/saphelp_gateway20sp12/2.0/en-US/cb/5dc700314e4e27be92de2d7065ce8e/content.htm?loaded_from_frameset=true


Configuration
-------------

**Reference Name:** Name used to uniquely identify this source for lineage, annotating metadata, etc.

**OData Service URL:** Root URL of the SAP OData service.
The URL must end with an external service name (e.g., http://eccsvrname:8000/sap/opu/odata/sap/zgw100_dd02l_so_srv/).

**Resource Path:** Path of the SAP OData entity set. For example: "SalesOrderCollection". For more information,
see [OData URL components].

**Query Options:** OData query options to filter the data. For more information, see [OData URL components].

[OData URL components]:
https://www.odata.org/documentation/odata-version-3-0/url-conventions/

**Username:** Username for basic authentication.

**Password:** Password for basic authentication.

**Output Schema:** Specifies the schema of the documents.


OData V2 Data Types Mapping
----------

    | OData V2 Data Type              | CDAP Schema Data Type                             |
    | ------------------------------- | ------------------------------------------------- |
    | Edm.Binary                      | bytes                                             |
    | Edm.Boolean                     | boolean                                           |
    | Edm.Byte                        | int                                               |
    | Edm.DateTime                    | timestamp                                         |
    | Edm.Decimal                     | decimal                                           |
    | Edm.Double                      | double                                            |
    | Edm.Single                      | float                                             |
    | Edm.Guid                        | string                                            |
    | Edm.Int16                       | int                                               |
    | Edm.Int32                       | int                                               |
    | Edm.Int64                       | long                                              |
    | Edm.SByte                       | int                                               |
    | Edm.String                      | string                                            |
    | Edm.Time                        | time                                              |
    | Edm.DateTimeOffset              | string formatted as 2019-08-29T14:52:08.155+02:00 |

For more information, see [OData V2 Primitive Data Types].

[OData V2 Primitive Data Types]:
https://www.odata.org/documentation/odata-version-2-0/overview/


OData V4 Data Types Mapping
----------

    | OData V4 Data Type              | CDAP Schema Data Type | Comment                                           |
    | ------------------------------- | --------------------- | ------------------------------------------------- |
    | Edm.Binary                      | bytes                 |                                                   |
    | Edm.Boolean                     | boolean               |                                                   |
    | Edm.Byte                        | int                   |                                                   |
    | Edm.DateTimeOffset              | string                | Timestamp string in the following format:         |
    |                                 |                       | 2019-08-29T14:52:08.155+02:00                     |
    | Edm.Decimal                     | decimal               |                                                   |
    | Edm.Double                      | double                |                                                   |
    | Edm.Guid                        | string                |                                                   |
    | Edm.Int16                       | int                   |                                                   |
    | Edm.Int32                       | int                   |                                                   |
    | Edm.Int64                       | long                  |                                                   |
    | Edm.SByte                       | int                   |                                                   |
    | Edm.Single                      | float                 |                                                   |
    | Edm.String                      | string                |                                                   |


For more information, see [OData V4 Primitive Data Types].

[The GeoJSON Format]:
https://tools.ietf.org/html/rfc7946
