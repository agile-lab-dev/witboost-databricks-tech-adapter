<!-- Generator: Widdershins v4.0.1 -->

<h1 id="specific-provisioner-micro-service">Specific Provisioner Micro Service v2.2.0</h1>

> Scroll down for code samples, example requests and responses. Select a language for code samples from the tabs above or the mobile navigation menu.

Microservice responsible to handle provisioning and access control requests for one or more data product components.

<h1 id="specific-provisioner-micro-service-specificprovisioner">SpecificProvisioner</h1>

All the provisioning related operations

## asyncValidate

<a id="opIdasyncValidate"></a>

> Code samples

```shell
# You can also use wget
curl -X POST /v2/validate \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json'

```

```javascript
const inputBody = '{
  "descriptorKind": "DATAPRODUCT_DESCRIPTOR",
  "descriptor": "string",
  "latestEnrichedDescriptor": "string",
  "removeData": false
}';
const headers = {
  'Content-Type':'application/json',
  'Accept':'application/json'
};

fetch('/v2/validate',
{
  method: 'POST',
  body: inputBody,
  headers: headers
})
.then(function(res) {
    return res.json();
}).then(function(body) {
    console.log(body);
});

```

```java
URL obj = new URL("/v2/validate");
HttpURLConnection con = (HttpURLConnection) obj.openConnection();
con.setRequestMethod("POST");
int responseCode = con.getResponseCode();
BufferedReader in = new BufferedReader(
    new InputStreamReader(con.getInputStream()));
String inputLine;
StringBuffer response = new StringBuffer();
while ((inputLine = in.readLine()) != null) {
    response.append(inputLine);
}
in.close();
System.out.println(response.toString());

```

```python
import requests
headers = {
  'Content-Type': 'application/json',
  'Accept': 'application/json'
}

r = requests.post('/v2/validate', headers = headers)

print(r.json())

```

`POST /v2/validate`

*Asynchronously validate a provisioning request*

> Body parameter

```json
{
  "descriptorKind": "DATAPRODUCT_DESCRIPTOR",
  "descriptor": "string",
  "latestEnrichedDescriptor": "string",
  "removeData": false
}
```

<h3 id="asyncvalidate-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|body|body|[ProvisioningRequest](#schemaprovisioningrequest)|true|Details of a provisioning request to be validated|

> Example responses

> 202 Response

```json
"string"
```

<h3 id="asyncvalidate-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|202|[Accepted](https://tools.ietf.org/html/rfc7231#section-6.3.3)|It returns a token that can be used for polling the async validation operation status and results|string|
|400|[Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)|Invalid input|[RequestValidationError](#schemarequestvalidationerror)|
|500|[Internal Server Error](https://tools.ietf.org/html/rfc7231#section-6.6.1)|System problem|[SystemError](#schemasystemerror)|

<aside class="success">
This operation does not require authentication
</aside>

## getValidationStatus

<a id="opIdgetValidationStatus"></a>

> Code samples

```shell
# You can also use wget
curl -X GET /v2/validate/{token}/status \
  -H 'Accept: application/json'

```

```javascript

const headers = {
  'Accept':'application/json'
};

fetch('/v2/validate/{token}/status',
{
  method: 'GET',

  headers: headers
})
.then(function(res) {
    return res.json();
}).then(function(body) {
    console.log(body);
});

```

```java
URL obj = new URL("/v2/validate/{token}/status");
HttpURLConnection con = (HttpURLConnection) obj.openConnection();
con.setRequestMethod("GET");
int responseCode = con.getResponseCode();
BufferedReader in = new BufferedReader(
    new InputStreamReader(con.getInputStream()));
String inputLine;
StringBuffer response = new StringBuffer();
while ((inputLine = in.readLine()) != null) {
    response.append(inputLine);
}
in.close();
System.out.println(response.toString());

```

```python
import requests
headers = {
  'Accept': 'application/json'
}

r = requests.get('/v2/validate/{token}/status', headers = headers)

print(r.json())

```

`GET /v2/validate/{token}/status`

*Get status and results of an async validation operation*

<h3 id="getvalidationstatus-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|token|path|string|true|token that identifies the request|

> Example responses

> 200 Response

```json
{
  "status": "RUNNING",
  "info": {
    "publicInfo": {
      "valid": true,
      "error": {
        "errors": [
          "string"
        ]
      }
    }
  }
}
```

<h3 id="getvalidationstatus-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|The request status|[ValidationStatus](#schemavalidationstatus)|
|400|[Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)|Invalid input|[RequestValidationError](#schemarequestvalidationerror)|
|500|[Internal Server Error](https://tools.ietf.org/html/rfc7231#section-6.6.1)|System problem|[SystemError](#schemasystemerror)|

<aside class="success">
This operation does not require authentication
</aside>

## provision

<a id="opIdprovision"></a>

> Code samples

```shell
# You can also use wget
curl -X POST /v1/provision \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json'

```

```javascript
const inputBody = '{
  "descriptorKind": "DATAPRODUCT_DESCRIPTOR",
  "descriptor": "string",
  "latestEnrichedDescriptor": "string",
  "removeData": false
}';
const headers = {
  'Content-Type':'application/json',
  'Accept':'application/json'
};

fetch('/v1/provision',
{
  method: 'POST',
  body: inputBody,
  headers: headers
})
.then(function(res) {
    return res.json();
}).then(function(body) {
    console.log(body);
});

```

```java
URL obj = new URL("/v1/provision");
HttpURLConnection con = (HttpURLConnection) obj.openConnection();
con.setRequestMethod("POST");
int responseCode = con.getResponseCode();
BufferedReader in = new BufferedReader(
    new InputStreamReader(con.getInputStream()));
String inputLine;
StringBuffer response = new StringBuffer();
while ((inputLine = in.readLine()) != null) {
    response.append(inputLine);
}
in.close();
System.out.println(response.toString());

```

```python
import requests
headers = {
  'Content-Type': 'application/json',
  'Accept': 'application/json'
}

r = requests.post('/v1/provision', headers = headers)

print(r.json())

```

`POST /v1/provision`

*Deploy a data product or a single component starting from a provisioning descriptor*

> Body parameter

```json
{
  "descriptorKind": "DATAPRODUCT_DESCRIPTOR",
  "descriptor": "string",
  "latestEnrichedDescriptor": "string",
  "removeData": false
}
```

<h3 id="provision-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|body|body|[ProvisioningRequest](#schemaprovisioningrequest)|true|Provisioning descriptor|

> Example responses

> 200 Response

```json
{
  "status": "RUNNING",
  "result": "string",
  "info": {
    "publicInfo": {},
    "privateInfo": {}
  },
  "logs": [
    {
      "timestamp": "2019-08-24T14:15:22Z",
      "level": "DEBUG",
      "message": "string",
      "phase": "string"
    }
  ]
}
```

<h3 id="provision-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|It synchronously returns the request result|[ProvisioningStatus](#schemaprovisioningstatus)|
|202|[Accepted](https://tools.ietf.org/html/rfc7231#section-6.3.3)|If successful returns a provisioning deployment task token that can be used for polling the request status|string|
|400|[Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)|Invalid input|[RequestValidationError](#schemarequestvalidationerror)|
|500|[Internal Server Error](https://tools.ietf.org/html/rfc7231#section-6.6.1)|System problem|[SystemError](#schemasystemerror)|

<aside class="success">
This operation does not require authentication
</aside>

## getStatus

<a id="opIdgetStatus"></a>

> Code samples

```shell
# You can also use wget
curl -X GET /v1/provision/{token}/status \
  -H 'Accept: application/json'

```

```javascript

const headers = {
  'Accept':'application/json'
};

fetch('/v1/provision/{token}/status',
{
  method: 'GET',

  headers: headers
})
.then(function(res) {
    return res.json();
}).then(function(body) {
    console.log(body);
});

```

```java
URL obj = new URL("/v1/provision/{token}/status");
HttpURLConnection con = (HttpURLConnection) obj.openConnection();
con.setRequestMethod("GET");
int responseCode = con.getResponseCode();
BufferedReader in = new BufferedReader(
    new InputStreamReader(con.getInputStream()));
String inputLine;
StringBuffer response = new StringBuffer();
while ((inputLine = in.readLine()) != null) {
    response.append(inputLine);
}
in.close();
System.out.println(response.toString());

```

```python
import requests
headers = {
  'Accept': 'application/json'
}

r = requests.get('/v1/provision/{token}/status', headers = headers)

print(r.json())

```

`GET /v1/provision/{token}/status`

*Get the status of a provisioning request*

<h3 id="getstatus-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|token|path|string|true|token that identifies the request|

> Example responses

> 200 Response

```json
{
  "status": "RUNNING",
  "result": "string",
  "info": {
    "publicInfo": {},
    "privateInfo": {}
  },
  "logs": [
    {
      "timestamp": "2019-08-24T14:15:22Z",
      "level": "DEBUG",
      "message": "string",
      "phase": "string"
    }
  ]
}
```

<h3 id="getstatus-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|The request status|[ProvisioningStatus](#schemaprovisioningstatus)|
|400|[Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)|Invalid input|[RequestValidationError](#schemarequestvalidationerror)|
|500|[Internal Server Error](https://tools.ietf.org/html/rfc7231#section-6.6.1)|System problem|[SystemError](#schemasystemerror)|

<aside class="success">
This operation does not require authentication
</aside>

## validate

<a id="opIdvalidate"></a>

> Code samples

```shell
# You can also use wget
curl -X POST /v1/validate \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json'

```

```javascript
const inputBody = '{
  "descriptorKind": "DATAPRODUCT_DESCRIPTOR",
  "descriptor": "string",
  "latestEnrichedDescriptor": "string",
  "removeData": false
}';
const headers = {
  'Content-Type':'application/json',
  'Accept':'application/json'
};

fetch('/v1/validate',
{
  method: 'POST',
  body: inputBody,
  headers: headers
})
.then(function(res) {
    return res.json();
}).then(function(body) {
    console.log(body);
});

```

```java
URL obj = new URL("/v1/validate");
HttpURLConnection con = (HttpURLConnection) obj.openConnection();
con.setRequestMethod("POST");
int responseCode = con.getResponseCode();
BufferedReader in = new BufferedReader(
    new InputStreamReader(con.getInputStream()));
String inputLine;
StringBuffer response = new StringBuffer();
while ((inputLine = in.readLine()) != null) {
    response.append(inputLine);
}
in.close();
System.out.println(response.toString());

```

```python
import requests
headers = {
  'Content-Type': 'application/json',
  'Accept': 'application/json'
}

r = requests.post('/v1/validate', headers = headers)

print(r.json())

```

`POST /v1/validate`

*Validate a provisioning request*

> Body parameter

```json
{
  "descriptorKind": "DATAPRODUCT_DESCRIPTOR",
  "descriptor": "string",
  "latestEnrichedDescriptor": "string",
  "removeData": false
}
```

<h3 id="validate-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|body|body|[ProvisioningRequest](#schemaprovisioningrequest)|true|Details of a provisioning request to be validated|

> Example responses

> 200 Response

```json
{
  "valid": true,
  "error": {
    "errors": [
      "string"
    ]
  }
}
```

<h3 id="validate-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|It synchronously returns a specific reply containing the validation result|[ValidationResult](#schemavalidationresult)|
|500|[Internal Server Error](https://tools.ietf.org/html/rfc7231#section-6.6.1)|System problem|[SystemError](#schemasystemerror)|

<aside class="success">
This operation does not require authentication
</aside>

## unprovision

<a id="opIdunprovision"></a>

> Code samples

```shell
# You can also use wget
curl -X POST /v1/unprovision \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json'

```

```javascript
const inputBody = '{
  "descriptorKind": "DATAPRODUCT_DESCRIPTOR",
  "descriptor": "string",
  "latestEnrichedDescriptor": "string",
  "removeData": false
}';
const headers = {
  'Content-Type':'application/json',
  'Accept':'application/json'
};

fetch('/v1/unprovision',
{
  method: 'POST',
  body: inputBody,
  headers: headers
})
.then(function(res) {
    return res.json();
}).then(function(body) {
    console.log(body);
});

```

```java
URL obj = new URL("/v1/unprovision");
HttpURLConnection con = (HttpURLConnection) obj.openConnection();
con.setRequestMethod("POST");
int responseCode = con.getResponseCode();
BufferedReader in = new BufferedReader(
    new InputStreamReader(con.getInputStream()));
String inputLine;
StringBuffer response = new StringBuffer();
while ((inputLine = in.readLine()) != null) {
    response.append(inputLine);
}
in.close();
System.out.println(response.toString());

```

```python
import requests
headers = {
  'Content-Type': 'application/json',
  'Accept': 'application/json'
}

r = requests.post('/v1/unprovision', headers = headers)

print(r.json())

```

`POST /v1/unprovision`

*Undeploy a data product or a single component given the provisioning descriptor relative to the latest complete provisioning request*

> Body parameter

```json
{
  "descriptorKind": "DATAPRODUCT_DESCRIPTOR",
  "descriptor": "string",
  "latestEnrichedDescriptor": "string",
  "removeData": false
}
```

<h3 id="unprovision-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|body|body|[ProvisioningRequest](#schemaprovisioningrequest)|true|Provisioning descriptor|

> Example responses

> 200 Response

```json
{
  "status": "RUNNING",
  "result": "string",
  "info": {
    "publicInfo": {},
    "privateInfo": {}
  },
  "logs": [
    {
      "timestamp": "2019-08-24T14:15:22Z",
      "level": "DEBUG",
      "message": "string",
      "phase": "string"
    }
  ]
}
```

<h3 id="unprovision-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|It synchronously returns the request result|[ProvisioningStatus](#schemaprovisioningstatus)|
|202|[Accepted](https://tools.ietf.org/html/rfc7231#section-6.3.3)|If successful returns a provisioning deployment task token that can be used for polling the request status|string|
|400|[Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)|Invalid input|[RequestValidationError](#schemarequestvalidationerror)|
|500|[Internal Server Error](https://tools.ietf.org/html/rfc7231#section-6.6.1)|System problem|[SystemError](#schemasystemerror)|

<aside class="success">
This operation does not require authentication
</aside>

## updateacl

<a id="opIdupdateacl"></a>

> Code samples

```shell
# You can also use wget
curl -X POST /v1/updateacl \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json'

```

```javascript
const inputBody = '{
  "refs": [
    "user:alice",
    "user:bob",
    "group:groupA",
    "group:groupB",
    "group:groupC"
  ],
  "provisionInfo": {
    "request": "string",
    "result": "string"
  }
}';
const headers = {
  'Content-Type':'application/json',
  'Accept':'application/json'
};

fetch('/v1/updateacl',
{
  method: 'POST',
  body: inputBody,
  headers: headers
})
.then(function(res) {
    return res.json();
}).then(function(body) {
    console.log(body);
});

```

```java
URL obj = new URL("/v1/updateacl");
HttpURLConnection con = (HttpURLConnection) obj.openConnection();
con.setRequestMethod("POST");
int responseCode = con.getResponseCode();
BufferedReader in = new BufferedReader(
    new InputStreamReader(con.getInputStream()));
String inputLine;
StringBuffer response = new StringBuffer();
while ((inputLine = in.readLine()) != null) {
    response.append(inputLine);
}
in.close();
System.out.println(response.toString());

```

```python
import requests
headers = {
  'Content-Type': 'application/json',
  'Accept': 'application/json'
}

r = requests.post('/v1/updateacl', headers = headers)

print(r.json())

```

`POST /v1/updateacl`

*Request the access to a specific provisioner component*

> Body parameter

```json
{
  "refs": [
    "user:alice",
    "user:bob",
    "group:groupA",
    "group:groupB",
    "group:groupC"
  ],
  "provisionInfo": {
    "request": "string",
    "result": "string"
  }
}
```

<h3 id="updateacl-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|body|body|[UpdateAclRequest](#schemaupdateaclrequest)|true|An access request object. The provisoning info reported in `provisionInfo` refer to the latest complete provisioning workflow of the target component|

> Example responses

> 200 Response

```json
{
  "status": "RUNNING",
  "result": "string",
  "info": {
    "publicInfo": {},
    "privateInfo": {}
  },
  "logs": [
    {
      "timestamp": "2019-08-24T14:15:22Z",
      "level": "DEBUG",
      "message": "string",
      "phase": "string"
    }
  ]
}
```

<h3 id="updateacl-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|It synchronously returns the access request response|[ProvisioningStatus](#schemaprovisioningstatus)|
|202|[Accepted](https://tools.ietf.org/html/rfc7231#section-6.3.3)|If successful returns a provisioning deployment task token that can be used for polling the request status|string|
|400|[Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)|Invalid input|[RequestValidationError](#schemarequestvalidationerror)|
|500|[Internal Server Error](https://tools.ietf.org/html/rfc7231#section-6.6.1)|System problem|[SystemError](#schemasystemerror)|

<aside class="success">
This operation does not require authentication
</aside>

## runReverseProvisioning

<a id="opIdrunReverseProvisioning"></a>

> Code samples

```shell
# You can also use wget
curl -X POST /v1/reverse-provisioning \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json'

```

```javascript
const inputBody = '{
  "useCaseTemplateId": "urn:dmb:utm:op-standard:0.0.0",
  "environment": "production",
  "params": {
    "inputA": "value A",
    "inputB": 1
  },
  "catalogInfo": {}
}';
const headers = {
  'Content-Type':'application/json',
  'Accept':'application/json'
};

fetch('/v1/reverse-provisioning',
{
  method: 'POST',
  body: inputBody,
  headers: headers
})
.then(function(res) {
    return res.json();
}).then(function(body) {
    console.log(body);
});

```

```java
URL obj = new URL("/v1/reverse-provisioning");
HttpURLConnection con = (HttpURLConnection) obj.openConnection();
con.setRequestMethod("POST");
int responseCode = con.getResponseCode();
BufferedReader in = new BufferedReader(
    new InputStreamReader(con.getInputStream()));
String inputLine;
StringBuffer response = new StringBuffer();
while ((inputLine = in.readLine()) != null) {
    response.append(inputLine);
}
in.close();
System.out.println(response.toString());

```

```python
import requests
headers = {
  'Content-Type': 'application/json',
  'Accept': 'application/json'
}

r = requests.post('/v1/reverse-provisioning', headers = headers)

print(r.json())

```

`POST /v1/reverse-provisioning`

*Execute a reverse provisioning operation*

> Body parameter

```json
{
  "useCaseTemplateId": "urn:dmb:utm:op-standard:0.0.0",
  "environment": "production",
  "params": {
    "inputA": "value A",
    "inputB": 1
  },
  "catalogInfo": {}
}
```

<h3 id="runreverseprovisioning-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|body|body|[ReverseProvisioningRequest](#schemareverseprovisioningrequest)|true|none|

> Example responses

> 200 Response

```json
{
  "status": "RUNNING",
  "updates": {
    "metadata.fieldA": "Value A",
    "spec.mesh.description": "Updated value",
    "spec.fieldB": {
      "subfieldA": "Value A",
      "subfieldB": 1
    }
  },
  "logs": [
    {
      "timestamp": "2019-08-24T14:15:22Z",
      "level": "DEBUG",
      "message": "string",
      "phase": "string"
    }
  ]
}
```

<h3 id="runreverseprovisioning-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|It synchronously returns the reverse provisioning response|[ReverseProvisioningStatus](#schemareverseprovisioningstatus)|
|202|[Accepted](https://tools.ietf.org/html/rfc7231#section-6.3.3)|It returns a reverse provisioning task token that can be used for polling the request status|string|
|400|[Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)|Invalid input|[RequestValidationError](#schemarequestvalidationerror)|
|500|[Internal Server Error](https://tools.ietf.org/html/rfc7231#section-6.6.1)|System problem|[SystemError](#schemasystemerror)|

<aside class="success">
This operation does not require authentication
</aside>

## getReverseProvisioningStatus

<a id="opIdgetReverseProvisioningStatus"></a>

> Code samples

```shell
# You can also use wget
curl -X GET /v1/reverse-provisioning/{token}/status \
  -H 'Accept: application/json'

```

```javascript

const headers = {
  'Accept':'application/json'
};

fetch('/v1/reverse-provisioning/{token}/status',
{
  method: 'GET',

  headers: headers
})
.then(function(res) {
    return res.json();
}).then(function(body) {
    console.log(body);
});

```

```java
URL obj = new URL("/v1/reverse-provisioning/{token}/status");
HttpURLConnection con = (HttpURLConnection) obj.openConnection();
con.setRequestMethod("GET");
int responseCode = con.getResponseCode();
BufferedReader in = new BufferedReader(
    new InputStreamReader(con.getInputStream()));
String inputLine;
StringBuffer response = new StringBuffer();
while ((inputLine = in.readLine()) != null) {
    response.append(inputLine);
}
in.close();
System.out.println(response.toString());

```

```python
import requests
headers = {
  'Accept': 'application/json'
}

r = requests.get('/v1/reverse-provisioning/{token}/status', headers = headers)

print(r.json())

```

`GET /v1/reverse-provisioning/{token}/status`

*Get status and results of a reverse provisioning operation*

<h3 id="getreverseprovisioningstatus-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|token|path|string|true|token that identifies the request|

> Example responses

> 200 Response

```json
{
  "status": "RUNNING",
  "updates": {
    "metadata.fieldA": "Value A",
    "spec.mesh.description": "Updated value",
    "spec.fieldB": {
      "subfieldA": "Value A",
      "subfieldB": 1
    }
  },
  "logs": [
    {
      "timestamp": "2019-08-24T14:15:22Z",
      "level": "DEBUG",
      "message": "string",
      "phase": "string"
    }
  ]
}
```

<h3 id="getreverseprovisioningstatus-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|The request status and results|[ReverseProvisioningStatus](#schemareverseprovisioningstatus)|
|400|[Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)|Invalid input|[RequestValidationError](#schemarequestvalidationerror)|
|500|[Internal Server Error](https://tools.ietf.org/html/rfc7231#section-6.6.1)|System problem|[SystemError](#schemasystemerror)|

<aside class="success">
This operation does not require authentication
</aside>

# Schemas

<h2 id="tocS_ValidationStatus">ValidationStatus</h2>
<!-- backwards compatibility -->
<a id="schemavalidationstatus"></a>
<a id="schema_ValidationStatus"></a>
<a id="tocSvalidationstatus"></a>
<a id="tocsvalidationstatus"></a>

```json
{
  "status": "RUNNING",
  "info": {
    "publicInfo": {
      "valid": true,
      "error": {
        "errors": [
          "string"
        ]
      }
    }
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|status|string|true|none|- `RUNNING`: the validation task is still running<br>- `COMPLETED`: the operation is complete and the validation result is available (`info.publicInfo` field)<br>- `FAILED`: an exception occurred while running the validation task|
|info|[ValidationInfo](#schemavalidationinfo)|false|none|none|

#### Enumerated Values

|Property|Value|
|---|---|
|status|RUNNING|
|status|COMPLETED|
|status|FAILED|

<h2 id="tocS_UpdateAclRequest">UpdateAclRequest</h2>
<!-- backwards compatibility -->
<a id="schemaupdateaclrequest"></a>
<a id="schema_UpdateAclRequest"></a>
<a id="tocSupdateaclrequest"></a>
<a id="tocsupdateaclrequest"></a>

```json
{
  "refs": [
    "user:alice",
    "user:bob",
    "group:groupA",
    "group:groupB",
    "group:groupC"
  ],
  "provisionInfo": {
    "request": "string",
    "result": "string"
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|refs|[string]|true|none|Identities (i.e. users and groups) involved in the ACL update request|
|provisionInfo|[ProvisionInfo](#schemaprovisioninfo)|true|none|Information related to the provisioning workflow of a data product component|

<h2 id="tocS_DescriptorKind">DescriptorKind</h2>
<!-- backwards compatibility -->
<a id="schemadescriptorkind"></a>
<a id="schema_DescriptorKind"></a>
<a id="tocSdescriptorkind"></a>
<a id="tocsdescriptorkind"></a>

```json
"DATAPRODUCT_DESCRIPTOR"

```

Values:
 * `DATAPRODUCT_DESCRIPTOR` - Complete descriptor of a data product. It is used in the data product level provisioning workflow.
 * `COMPONENT_DESCRIPTOR` - Provisioning descriptor for a single data product component. Includes both the complete data product descriptor (`dataProduct` object field) and the id of the component to be provisioned (`componentIdToProvision` string field).
 * `DATAPRODUCT_DESCRIPTOR_WITH_RESULTS` - This value is not currently used in the scope of a specific provisioner.

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|string|false|none|Values:<br> * `DATAPRODUCT_DESCRIPTOR` - Complete descriptor of a data product. It is used in the data product level provisioning workflow.<br> * `COMPONENT_DESCRIPTOR` - Provisioning descriptor for a single data product component. Includes both the complete data product descriptor (`dataProduct` object field) and the id of the component to be provisioned (`componentIdToProvision` string field).<br> * `DATAPRODUCT_DESCRIPTOR_WITH_RESULTS` - This value is not currently used in the scope of a specific provisioner.|

#### Enumerated Values

|Property|Value|
|---|---|
|*anonymous*|DATAPRODUCT_DESCRIPTOR|
|*anonymous*|COMPONENT_DESCRIPTOR|
|*anonymous*|DATAPRODUCT_DESCRIPTOR_WITH_RESULTS|

<h2 id="tocS_ProvisioningRequest">ProvisioningRequest</h2>
<!-- backwards compatibility -->
<a id="schemaprovisioningrequest"></a>
<a id="schema_ProvisioningRequest"></a>
<a id="tocSprovisioningrequest"></a>
<a id="tocsprovisioningrequest"></a>

```json
{
  "descriptorKind": "DATAPRODUCT_DESCRIPTOR",
  "descriptor": "string",
  "latestEnrichedDescriptor": "string",
  "removeData": false
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|descriptorKind|[DescriptorKind](#schemadescriptorkind)|true|none|Values:<br> * `DATAPRODUCT_DESCRIPTOR` - Complete descriptor of a data product. It is used in the data product level provisioning workflow.<br> * `COMPONENT_DESCRIPTOR` - Provisioning descriptor for a single data product component. Includes both the complete data product descriptor (`dataProduct` object field) and the id of the component to be provisioned (`componentIdToProvision` string field).<br> * `DATAPRODUCT_DESCRIPTOR_WITH_RESULTS` - This value is not currently used in the scope of a specific provisioner.|
|descriptor|string|true|none|Descriptor specification in yaml format. Its structure changes according to `descriptorKind`.|
|latestEnrichedDescriptor|string|false|none|*Filled only in case of provision/unprovison requests.*<br><br>Complete data product descriptor (YAML format), enriched with provisioning info provided by the specific provisioners during the latest (successful or failed) provisioning/unprovisioning operation for each component.  If available, provisioning information is reported in the `info` fields at both the data product and component levels. The base schema for the `info` object is the same as [Info](#/components/schemas/Info), but additional fields may be included, such as the `latestProvisioningOperation` object. This object contains information about the most recent provisioning operation performed on the component, including the operation type (e.g. Deploy, Undeploy) in the `operation` field, as well as the status of the operation (e.g. NotStarted, Failed, Successful) in the `status` field. Please note that the information contained in a `info` object represents the most recent data provided by a specific provisioner for the corresponding component. However, this information may not have been provided during the last data product provisioning operation. For example, if the provisioning operation fails and the component's specific provisioner is not called, the information doesn't change. Only the `latestProvisioningOperation` object always refers to the most recent provisioning operation on the data product.|
|removeData|boolean|true|none|If true, when a component is undeployed, its underlying data will also be deleted|

<h2 id="tocS_ProvisioningStatus">ProvisioningStatus</h2>
<!-- backwards compatibility -->
<a id="schemaprovisioningstatus"></a>
<a id="schema_ProvisioningStatus"></a>
<a id="tocSprovisioningstatus"></a>
<a id="tocsprovisioningstatus"></a>

```json
{
  "status": "RUNNING",
  "result": "string",
  "info": {
    "publicInfo": {},
    "privateInfo": {}
  },
  "logs": [
    {
      "timestamp": "2019-08-24T14:15:22Z",
      "level": "DEBUG",
      "message": "string",
      "phase": "string"
    }
  ]
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|status|string|true|none|none|
|result|string|true|none|none|
|info|[Info](#schemainfo)|false|none|none|
|logs|[[Log](#schemalog)]|false|none|none|

#### Enumerated Values

|Property|Value|
|---|---|
|status|RUNNING|
|status|COMPLETED|
|status|FAILED|

<h2 id="tocS_ValidationResult">ValidationResult</h2>
<!-- backwards compatibility -->
<a id="schemavalidationresult"></a>
<a id="schema_ValidationResult"></a>
<a id="tocSvalidationresult"></a>
<a id="tocsvalidationresult"></a>

```json
{
  "valid": true,
  "error": {
    "errors": [
      "string"
    ]
  }
}

```

Result of a validation operation on a provided descriptor

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|valid|boolean|true|none|Whether the provided descriptor is valid or not|
|error|[ValidationError](#schemavalidationerror)|false|none|Validation error related to a provided descriptor|

<h2 id="tocS_ProvisionInfo">ProvisionInfo</h2>
<!-- backwards compatibility -->
<a id="schemaprovisioninfo"></a>
<a id="schema_ProvisionInfo"></a>
<a id="tocSprovisioninfo"></a>
<a id="tocsprovisioninfo"></a>

```json
{
  "request": "string",
  "result": "string"
}

```

Information related to the provisioning workflow of a data product component

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|request|string|true|none|Provisioning descriptor of type `COMPONENT_DESCRIPTOR` (see [DescriptorKind](#/components/schemas/DescriptorKind) schema) in JSON format. It had been used to provision the data product component|
|result|string|true|none|Result message (e.g. a provisiong error or a success message returned by the specific provisioner in the [ProvisioningStatus](#/components/schemas/ProvisioningStatus))|

<h2 id="tocS_ValidationError">ValidationError</h2>
<!-- backwards compatibility -->
<a id="schemavalidationerror"></a>
<a id="schema_ValidationError"></a>
<a id="tocSvalidationerror"></a>
<a id="tocsvalidationerror"></a>

```json
{
  "errors": [
    "string"
  ]
}

```

Validation error related to a provided descriptor

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|errors|[string]|true|none|none|

<h2 id="tocS_RequestValidationError">RequestValidationError</h2>
<!-- backwards compatibility -->
<a id="schemarequestvalidationerror"></a>
<a id="schema_RequestValidationError"></a>
<a id="tocSrequestvalidationerror"></a>
<a id="tocsrequestvalidationerror"></a>

```json
{
  "errors": [
    "string"
  ],
  "userMessage": "string",
  "input": "string",
  "inputErrorField": "string",
  "moreInfo": {
    "problems": [
      "string"
    ],
    "solutions": [
      "string"
    ]
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|errors|[string]|true|none|none|
|userMessage|string|false|none|User-readable message to be displayed|
|input|string|false|none|Optional field to include the file or descriptor that raised the error|
|inputErrorField|string|false|none|Optional field to include the field path (in dot format) that raised the error|
|moreInfo|[ErrorMoreInfo](#schemaerrormoreinfo)|false|none|Object that will include the more in-depth, specific information about the error|

<h2 id="tocS_SystemError">SystemError</h2>
<!-- backwards compatibility -->
<a id="schemasystemerror"></a>
<a id="schema_SystemError"></a>
<a id="tocSsystemerror"></a>
<a id="tocssystemerror"></a>

```json
{
  "error": "string",
  "userMessage": "string",
  "input": "string",
  "inputErrorField": "string",
  "moreInfo": {
    "problems": [
      "string"
    ],
    "solutions": [
      "string"
    ]
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|error|string|true|none|none|
|userMessage|string|false|none|User-readable message to be displayed|
|input|string|false|none|Optional field to include the file or descriptor that raised the error|
|inputErrorField|string|false|none|Optional field to include the field path (in dot format) that raised the error|
|moreInfo|[ErrorMoreInfo](#schemaerrormoreinfo)|false|none|Object that will include the more in-depth, specific information about the error|

<h2 id="tocS_ErrorMoreInfo">ErrorMoreInfo</h2>
<!-- backwards compatibility -->
<a id="schemaerrormoreinfo"></a>
<a id="schema_ErrorMoreInfo"></a>
<a id="tocSerrormoreinfo"></a>
<a id="tocserrormoreinfo"></a>

```json
{
  "problems": [
    "string"
  ],
  "solutions": [
    "string"
  ]
}

```

Object that will include the more in-depth, specific information about the error

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|problems|[string]|true|none|Array of possible multiple problems: i.e. multiple validations failed|
|solutions|[string]|true|none|Array of possible solutions that the developer gives to the user to solve the issue|

<h2 id="tocS_Info">Info</h2>
<!-- backwards compatibility -->
<a id="schemainfo"></a>
<a id="schema_Info"></a>
<a id="tocSinfo"></a>
<a id="tocsinfo"></a>

```json
{
  "publicInfo": {},
  "privateInfo": {}
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|publicInfo|object|true|none|Fields to display in the Marketplace UI. Note that only the values compliant to specific structures will be rendered in the "Technical Information" card of the Marketplace pages. [Check the documentation](https://docs.internal.witboost.agilelab.it/docs/p3_tech/p3_customizations/p3_4_templates/infrastructureTemplate#specific-provisioner-api-details) for additional details|
|privateInfo|object|true|none|All the values in this object will be stored in the deployed descriptor, but will not be shown in the Marketplace UI|

<h2 id="tocS_ValidationInfo">ValidationInfo</h2>
<!-- backwards compatibility -->
<a id="schemavalidationinfo"></a>
<a id="schema_ValidationInfo"></a>
<a id="tocSvalidationinfo"></a>
<a id="tocsvalidationinfo"></a>

```json
{
  "publicInfo": {
    "valid": true,
    "error": {
      "errors": [
        "string"
      ]
    }
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|publicInfo|[ValidationResult](#schemavalidationresult)|true|none|Result of a validation operation on a provided descriptor|

<h2 id="tocS_Log">Log</h2>
<!-- backwards compatibility -->
<a id="schemalog"></a>
<a id="schema_Log"></a>
<a id="tocSlog"></a>
<a id="tocslog"></a>

```json
{
  "timestamp": "2019-08-24T14:15:22Z",
  "level": "DEBUG",
  "message": "string",
  "phase": "string"
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|timestamp|string(date-time)|true|none|none|
|level|string|true|none|This is the severity level of the log|
|message|string|true|none|none|
|phase|string|false|none|none|

#### Enumerated Values

|Property|Value|
|---|---|
|level|DEBUG|
|level|INFO|
|level|WARNING|
|level|ERROR|

<h2 id="tocS_ReverseProvisioningRequest">ReverseProvisioningRequest</h2>
<!-- backwards compatibility -->
<a id="schemareverseprovisioningrequest"></a>
<a id="schema_ReverseProvisioningRequest"></a>
<a id="tocSreverseprovisioningrequest"></a>
<a id="tocsreverseprovisioningrequest"></a>

```json
{
  "useCaseTemplateId": "urn:dmb:utm:op-standard:0.0.0",
  "environment": "production",
  "params": {
    "inputA": "value A",
    "inputB": 1
  },
  "catalogInfo": {}
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|useCaseTemplateId|string|true|none|Component's use case template id|
|environment|string|true|none|Target environment|
|params|object|false|none|Reverse provisioning input params|
|catalogInfo|object|false|none|Content of the current `catalog-info.yaml` of the component|

<h2 id="tocS_ReverseProvisioningStatus">ReverseProvisioningStatus</h2>
<!-- backwards compatibility -->
<a id="schemareverseprovisioningstatus"></a>
<a id="schema_ReverseProvisioningStatus"></a>
<a id="tocSreverseprovisioningstatus"></a>
<a id="tocsreverseprovisioningstatus"></a>

```json
{
  "status": "RUNNING",
  "updates": {
    "metadata.fieldA": "Value A",
    "spec.mesh.description": "Updated value",
    "spec.fieldB": {
      "subfieldA": "Value A",
      "subfieldB": 1
    }
  },
  "logs": [
    {
      "timestamp": "2019-08-24T14:15:22Z",
      "level": "DEBUG",
      "message": "string",
      "phase": "string"
    }
  ]
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|status|string|true|none|- `RUNNING`: the reverse provisioning operation is still running<br>- `COMPLETED`: the operation is complete and the `updates` object is ready<br>- `FAILED`: an exception occurred while running the reverse provisioning operation (additional details could be found in the `logs` array)|
|updates|object|true|none|Field updates to be applied to the componenent's `catalog-info.yaml`. See the Reverse Provisioning documentation to learn more about the syntax of this object.|
|logs|[[Log](#schemalog)]|false|none|none|

#### Enumerated Values

|Property|Value|
|---|---|
|status|RUNNING|
|status|COMPLETED|
|status|FAILED|

