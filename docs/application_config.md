# `application.yaml` Configuration

This `application.yaml` file is used to configure a Spring Boot-based application for integration with Databricks and Azure. Below is a detailed explanation of each section.

## `spring` Section

```yaml
spring:
  application:
    name: databricks-specific-provisioner
```

* **application.name**: Defines the name of the application. In this case, the application is called databricks-specific-provisioner.

## `server` Section

```yaml
server:
  port: 8888
```

* **server.port**: Defines the port the application will listen on. In this case, the port is 8888


## `springdoc` Section

```yaml
springdoc:
  swagger-ui:
    path: /docs
```

* **springdoc.swagger-ui.path**: Defines the path for the Swagger UI interface for API documentation. Here it is set to /docs.

## `azure` Section

This section of the configuration file manages the integration with Azure and includes two main parts: **Authentication** and **Permissions**.

#### Authentication

This section handles the data required for authentication with Azure and for creating and managing resources such as the Databricks Workspace.

```yaml
azure:
    auth:
    clientId: ${AZURE_CLIENT_ID}
    tenantId: ${AZURE_TENANT_ID}
    clientSecret: ${AZURE_CLIENT_SECRET}
    skuType: ToBeFilled
```

* **auth.clientId**: The client ID for Azure authentication, provided via `${AZURE_CLIENT_ID}`.
* **auth.tenantId**: The Azure tenant ID, provided via `${AZURE_TENANT_ID}`.
* **auth.clientSecret**: The client secret for Azure authentication, provided via `${AZURE_CLIENT_SECRET}`.
* **auth.skuType**: Specifies the SKU type of the new Workspace that will be created, allowed values are `PREMIUM` or `TRIAL`. If left blank or set to something else, it defaults to PREMIUM.

Note: All sensitive values are managed through environment variables. For further details on these variables, refer to [azure_databricks_config.md](azure_databricks_config.md).

#### Permissions

This section manages the permissions associated with the Workspace created during provisioning. It includes the authentication details needed to map groups and users to Azure identities and to assign permissions.
The provisioner uses a service principal to authenticate against Microsoft Graph API. ClientId, tenantId and clientSecret of the service principal are required.The following permissions are required for the service principal:
- `User.Read.All`
- `GroupMember.Read.All`

```yaml
    permissions:
      auth_clientId: ${PERMISSIONS_AZURE_CLIENT_ID}
      auth_tenantId: ${PERMISSIONS_AZURE_TENANT_ID}
      auth_clientSecret: ${PERMISSIONS_AZURE_CLIENT_SECRET}
      subscriptionId: ${AZURE_SUBSCRIPTION_ID}
      resourceGroup: ${AZURE_RESOURCE_GROUP_NAME}
      dpOwnerRoleDefinitionId: ToBeFilled
      devGroupRoleDefinitionId: ToBeFilled
```

* **permissions.auth_clientId**: The client ID of the service principal, provided via  `${PERMISSIONS_AZURE_CLIENT_ID}`.
* **permissions.auth_tenantId**: The Azure tenant ID of the service principal, provided via `${PERMISSIONS_AZURE_TENANT_ID}`.
* **permissions.auth_clientSecret**: The client secret of the service principal, provided via `${PERMISSIONS_AZURE_CLIENT_SECRET}`.
* **permissions.subscriptionId**: The Azure subscription ID, provided via `${AZURE_SUBSCRIPTION_ID}`. Used to construct the resource ID. If the Workspace already exists and should not be managed by the Tech Adapter, this value can be omitted as it's not used.
* **permissions.resourceGroup**: The Azure resource group name, provided via `${AZURE_RESOURCE_GROUP_NAME}`. Used to construct the resource ID. If the Workspace already exists and should not be managed by the Tech Adapter, this value can be omitted as it's not used.
* **permissions.dpOwnerRoleDefinitionId**: Specifies the role for the Data Product owner. It can be set to `"no_permissions"` or filled with an ID from Azure RBAC roles. If set to `"no_permissions"`, all direct permissions on the resource (not inherited ones) will be removed. If the Workspace already exists and should not be managed by the Tech Adapter, this value can be omitted as it's not used.
* **permissions.devGroupRoleDefinitionId**: Specifies the role for the Developer group. It can be set to `"no_permissions"` or filled with an ID from Azure RBAC roles. If set to `"no_permissions"`, all direct permissions on the resource (not inherited ones) will be removed. If the Workspace already exists and should not be managed by the Tech Adapter, this value can be omitted as it's not used.


## `databricks` Section

This section handles authentication and permissions for Databricks. 

The Tech Adapter is configured to create a Workspace as part of its deployment process, unless it receives as input in the `specific.workspace` field a [Databricks Workspace URL](https://learn.microsoft.com/en-us/azure/databricks/workspace/workspace-details#per-workspace-url) rather than the Workspace name.

```yaml
databricks:
    auth:
      accountId: ${DATABRICKS_ACCOUNT_ID}
    permissions:
        workload:
          owner: ToBeFilled       # CAN_EDIT, CAN_MANAGE, CAN_READ, CAN_RUN, NO_PERMISSIONS
          developer: ToBeFilled   # CAN_EDIT, CAN_MANAGE, CAN_READ, CAN_RUN, NO_PERMISSIONS
        outputPort:
          owner: ToBeFilled        #ALL PRIVILEGES, APPLY_TAG, SELECT
          developer: ToBeFilled     #ALL PRIVILEGES, APPLY_TAG, SELECT

```

* **auth.accountId**: The Databricks account ID, provided via `${DATABRICKS_ACCOUNT_ID}`.
* **permissions.workload.owner**: Defines the permission level for the data product owner for the workload-related Databricks repo. Options: `CAN_EDIT`, `CAN_MANAGE`, `CAN_READ`, `CAN_RUN`, `NO_PERMISSIONS`.
* **permissions.workload.developer**: Defines the permission level for the development group for the workload-related Databricks repo. Options: `CAN_EDIT`, `CAN_MANAGE`, `CAN_READ`, `CAN_RUN`, `NO_PERMISSIONS`.
* **permissions.outputPort.owner**: Defines the permission level for the data product owner for the Databricks output port. Options: `ALL_PRIVILEGES`, `APPLY_TAG`, `SELECT`.
* **permissions.outputPort.developer**: Defines the permission level for the developer group for the Databricks output port. Options: `ALL_PRIVILEGES`, `APPLY_TAG`, `SELECT`.


## `git` Section

This section is used for configure the Git integration with the Databricks workspace.

```yaml
git:
  username: ${GIT_USERNAME}
  token: ${GIT_TOKEN}
  provider: GITLAB

```

* **git.username**: The username for Git authentication, provided via `${GIT_USERNAME}`.
* **git.token**: The access token for Git, provided via `${GIT_TOKEN}`.
* **git.provider**: The Git provider, in this case set to `GITLAB`.


## `forkjoin` Section
```yaml
forkjoin:
  parallelism: ToBeFilled
```

* **forkjoin.parallelism**: Defines the parallelism level for the ForkJoin framework.


## `usecasetemplateid` Section

Expected useCaseTemplateId values in request bodies to identify the type of component that sent the request. The use case template id must be added without the version section of the id.

```yaml
usecasetemplateid:
  workload:
    job: ["urn:dmb:utm:databricks-workload-job-template"]
    dlt: ["urn:dmb:utm:databricks-workload-dlt-template"]
    workflow: ["urn:dmb:utm:databricks-workload-workflow-template"]
  outputport: ["urn:dmb:utm:databricks-outputport-template"]
```

* **workload.job**: useCaseTemplateId for Databricks jobs.
* **workload.dlt**: useCaseTemplateId for Databricks Delta Live Tables (DLT).
* **workload.workflow**: useCaseTemplateId for Databricks Workflows.
* **outputport**: useCaseTemplateId for Databricks Output Port.


## `misc` Section
```yaml
misc:
  developmentEnvironmentName: ToBeFilled
```
* **misc.developmentEnvironmentName**: The name of the development environment.
