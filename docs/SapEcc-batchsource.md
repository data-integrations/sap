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

**Include Metadata Annotations:** Whether the plugin should read SAP metadata annotations and include them to each
record. In this case, each property mapped to a record with exactly two fields:
- value - for OData property value.
- metadata-annotations - metadata annotations record.

OData V4 metadata annotations mapped to a record with fields that correspond to the property attributes.
For example, for property, annotated as:
```
<Property Name="BuyerName" Type="Edm.String" Nullable="false" MaxLength="80" sap:label="Company"
                  sap:creatable="false" sap:updatable="false" sap:sortable="false" sap:filterable="false"/>
```
an entry with property value `true` will be read as the following record:
```
value: true
metadata-annotations:
  creatable: false
  updatable: false
  sortable: false
  filterable: false
```

OData V4 metadata annotations mapped to a record with the following fields:
- term - a simple identifier, such as "UI.DisplayName" or "Core.Description", etc.
- qualifier - a term can be applied multiple times to the same model element by providing a qualifier to distinguish
 the annotations.
- expression - record that corresponds to a constant expression or a dynamic expression.
- annotations - record that corresponds to nested annotations.

For example, for property, annotated as:
```
<Property Name="RecordNestedAnnotated" Type="Edm.Boolean" Nullable="false">
  <Annotation Term="Core.Description">
    <Annotation Term="Core.Description" Qualifier="nested" String="Nested annotation">
    <Record>
      <Annotation Term="Core.Description" Qualifier="onrecord" String="Annotation on record" />
      <PropertyValue Property="GivenName" Path="String"/>
      <PropertyValue Property="Age" Path="Byte"/>
    </Record>
  </Annotation>
</Property>
```
an entry with property value `true` will be read as the following record:
```
value: true
metadata-annotations:
 core_description:
  term: "Core.Description"
  qualifier: null
  expression:
    name: "Record"
    type: null
    propertyValues:
      GivenName:
        name: "Path"
        value: "SomeProperty1"
      Age:
        name: "Path"
        value: "SomeProperty2"
    annotations:
      onrecord_core_description:
        term: "Core.Description"
        qualifier: "onrecord"
        expression:
          name: "String"
          value: "Annotation on record"
  annotations:
    nested_core_description:
      term: "Core.Description"
      qualifier: "nested"
      expression:
        name: "String"
        value: "Nested annotation"
```

For more information, see [OData V4 Annotations].

[OData V4 Annotations]:
https://docs.oasis-open.org/odata/odata-csdl-xml/v4.01/csprd05/odata-csdl-xml-v4.01-csprd05.html#sec_Annotation

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

    | OData V4 Data Type              | CDAP Schema Data Type | Comment                                             |
    | ------------------------------- | --------------------- | --------------------------------------------------- |
    | Edm.Binary                      | bytes                 |                                                     |
    | Edm.Boolean                     | boolean               |                                                     |
    | Edm.Byte                        | int                   |                                                     |
    | Edm.Date                        | timestamp             |                                                     |
    | Edm.DateTimeOffset              | string                | Timestamp string in the following format:           |
    |                                 |                       | 2019-08-29T14:52:08.155+02:00                       |
    | Edm.Decimal                     | decimal               |                                                     |
    | Edm.Double                      | double                |                                                     |
    | Edm.Duration                    | string                | String formatted as P12DT23H59M59.999999999999S     |
    | Edm.Guid                        | string                |                                                     |
    | Edm.Int16                       | int                   |                                                     |
    | Edm.Int32                       | int                   |                                                     |
    | Edm.Int64                       | long                  |                                                     |
    | Edm.SByte                       | int                   |                                                     |
    | Edm.Single                      | float                 |                                                     |
    | Edm.Stream                      | record                | Record of string fields:                            |
    |                                 |                       | "mediaReadLink" - link to read the stream           |
    |                                 |                       | "mediaEditLink" - link to edit/update the stream    |
    |                                 |                       | "mediaContentType" - media type of the stream       |
    |                                 |                       | "mediaEtag" - HTTP ETag of the stream               |
    | Edm.String                      | string                |                                                     |
    | Edm.TimeOfDay                   | time                  |                                                     |
    | Edm.GeographyPoint              | record                | Record of the following fields:                     |
    |                                 |                       | "dimension" - "GEOGRAPHY" dimension                 |
    |                                 |                       | "x" - coordinate on X axis                          |
    |                                 |                       | "y" - coordinate on Y axis                          |
    |                                 |                       | "z" - coordinate on Z axis                          |
    | Edm.GeographyLineString         | record                | Record of the following fields:                     |
    |                                 |                       | "type" - type of GeoJSON object                     |
    |                                 |                       | "dimension" - "GEOGRAPHY" dimension                 |
    |                                 |                       | "coordinates" - array of Edm.GeographyPoint records |
    | Edm.GeographyPolygon            | record                | Record of the following fields:                     |
    |                                 |                       | "type" - type of GeoJSON object                     |
    |                                 |                       | "dimension" - "GEOGRAPHY" dimension                 |
    |                                 |                       | "exterior" - array of Edm.GeographyPoint records    |
    |                                 |                       | "interior" - array of Edm.GeographyLineString       |
    |                                 |                       | records                                             |
    |                                 |                       | "numberOfInteriorRings" - number of interior rings  |
    | Edm.GeographyMultiPoint         | record                | Record of the following fields:                     |
    |                                 |                       | "type" - type of GeoJSON object                     |
    |                                 |                       | "dimension" - "GEOGRAPHY" dimension                 |
    |                                 |                       | "coordinates" - array of Edm.GeographyPoint records |
    | Edm.GeographyMultiLineString    | record                | Record of the following fields:                     |
    |                                 |                       | "type" - type of GeoJSON object                     |
    |                                 |                       | "dimension" - "GEOGRAPHY" dimension                 |
    |                                 |                       | "coordinates" - array of Edm.GeographyLineString    |
    |                                 |                       | records                                             |
    | Edm.GeographyMultiPolygon       | record                | Record of the following fields:                     |
    |                                 |                       | "type" - type of GeoJSON object                     |
    |                                 |                       | "dimension" - "GEOGRAPHY" dimension                 |
    |                                 |                       | "coordinates" - array of Edm.GeographyPolygon       |
    |                                 |                       | records                                             |
    | Edm.GeographyCollection         | record                | Record of the following fields:                     |
    |                                 |                       | "type" - type of GeoJSON object                     |
    |                                 |                       | "dimension" - "GEOGRAPHY" dimension                 |
    |                                 |                       | "points" - array of Edm.GeographyPoint records      |
    |                                 |                       | "lineStrings" - array of Edm.GeographyLineString    |
    |                                 |                       | records                                             |
    |                                 |                       | "polygons" - array of Edm.GeographyPolygon records  |
    |                                 |                       | "multiPoints" - array of Edm.GeographyMultiPoint    |
    |                                 |                       | records                                             |
    |                                 |                       | "multiLineStrings" - array of                       |
    |                                 |                       | Edm.GeographyMultiLineString records                |
    |                                 |                       | "multiPolygons" - array of                          |
    |                                 |                       | Edm.GeographyMultiPolygon records                   |
    | Edm.GeometryPoint               | record                | Record of the following fields:                     |
    |                                 |                       | "dimension" - "GEOMETRY" dimension                  |
    |                                 |                       | "x" - coordinate on X axis                          |
    |                                 |                       | "y" - coordinate on Y axis                          |
    |                                 |                       | "z" - coordinate on Z axis                          |
    | Edm.GeometryLineString          | record                | Record of the following fields:                     |
    |                                 |                       | "type" - type of GeoJSON object                     |
    |                                 |                       | "dimension" - "GEOMETRY" dimension                  |
    |                                 |                       | "coordinates" - array of Edm.GeometryPoint records  |
    | Edm.GeometryPolygon             | record                | Record of the following fields:                     |
    |                                 |                       | "type" - type of GeoJSON object                     |
    |                                 |                       | "dimension" - "GEOMETRY" dimension                  |
    |                                 |                       | "exterior" - array of Edm.GeometryPoint records     |
    |                                 |                       | "interior" - array of Edm.GeometryLineString        |
    |                                 |                       | records                                             |
    |                                 |                       | "numberOfInteriorRings" - number of interior rings  |
    | Edm.GeometryMultiPoint          | record                | Record of the following fields:                     |
    |                                 |                       | "type" - type of GeoJSON object                     |
    |                                 |                       | "dimension" - "GEOMETRY" dimension                  |
    |                                 |                       | "coordinates" - array of Edm.GeometryPoint records  |
    | Edm.GeometryMultiLineString     | record                | Record of the following fields:                     |
    |                                 |                       | "type" - type of GeoJSON object                     |
    |                                 |                       | "dimension" - "GEOMETRY" dimension                  |
    |                                 |                       | "coordinates" - array of Edm.GeometryLineString     |
    |                                 |                       | records                                             |
    | Edm.GeometryMultiPolygon        | record                | Record of the following fields:                     |
    |                                 |                       | "type" - type of GeoJSON object                     |
    |                                 |                       | "dimension" - "GEOMETRY" dimension                  |
    |                                 |                       | "coordinates" - array of Edm.GeometryPolygon        |
    |                                 |                       | records                                             |
    | Edm.GeometryCollection          | record                | Record of the following fields:                     |
    |                                 |                       | "type" - type of GeoJSON object                     |
    |                                 |                       | "dimension" - "GEOMETRY" dimension                  |
    |                                 |                       | "points" - array of Edm.GeometryPoint records       |
    |                                 |                       | "lineStrings" - array of Edm.GeometryLineString     |
    |                                 |                       | records                                             |
    |                                 |                       | "polygons" - array of Edm.GeometryPolygon records   |
    |                                 |                       | "multiPoints" - array of Edm.GeometryMultiPoint     |
    |                                 |                       | records                                             |
    |                                 |                       | "multiLineStrings" - array of                       |
    |                                 |                       | Edm.GeometryMultiLineString records                 |
    |                                 |                       | "multiPolygons" - array of Edm.GeometryMultiPolygon |
    |                                 |                       | records                                             |


For more information, see [OData V4 Primitive Data Types], [The GeoJSON Format].

[The GeoJSON Format]:
https://tools.ietf.org/html/rfc7946

[OData V4 Primitive Data Types]:
https://docs.oasis-open.org/odata/odata-csdl-xml/v4.01/csprd05/odata-csdl-xml-v4.01-csprd05.html#sec_PrimitiveTypes
