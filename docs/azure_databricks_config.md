# Azure Databricks Configuration

This is an initial documentation file to remind you of the steps required to enable the provisioner to create and manage Databricks Workspaces in Azure. The file will be updated shortly.

Complete these steps to enable the full functionality of the provisioner:

## Step 1: Azure Databricks Account Admin Setup

To dynamically create Databricks Workspaces, the Azure user must be a Databricks account admin. Only the global administrator can enable them by following these instructions:

1. Sign into your Azure Portal with your Global Admin credentials.
2. Go to [https://accounts.azuredatabricks.net/](https://accounts.azuredatabricks.net/) and sign in with Microsoft Entra ID. Azure Databricks automatically creates an account admin role for you.
3. Click User management.
4. Find and click the username of the user you want to delegate the account admin role to.
5. On the Roles tab, turn on Account admin.

## Step 2: Create a Microsoft Entra ID service principal in your Azure account
1. Sign in to the [Azure portal](https://portal.azure.com/#home).
2. In **Search resources, services, and docs**, search for and select **Microsoft Entra ID**.
3. Click **Add** and select **App registration**.
4. For **Name**, enter a name for the application.
5. In the **Supported account types section**, select **Accounts in this organizational directory only (Single tenant)**.
6. Click **Register**.
7. On the application page’s **Overview** page, in the **Essentials** section, copy the following values:
   - **Application (client) ID**
   - **Directory (tenant) ID**
8. To generate a client secret, within **Manage**, click **Certificates & secrets**.
9. On the **Client secrets** tab, click **New client secret**.
10. In the **Add a client secret** pane, for **Description**, enter a description for the client secret.
11. For **Expires**, select an expiry time period for the client secret, and then click **Add**.
12. Copy and store the client secret’s **Value** in a secure place, as this client secret is the password for your application.

## Step 3: Set environment variables

Authentication requires the setup of this environment variables:
- **AZURE_CLIENT_ID**
- **AZURE_TENANT_ID**
- **AZURE_CLIENT_SECRET**
- **AZURE_SUBSCRIPTION_ID**: You can find this in your Azure subscription details. To locate it:
   1. Access the [Azure Portal](https://portal.azure.com/).
   2. In the **search bar**, type *subscriptions* and select **Subscriptions** from the results.
   3. Choose the specific subscription for which you need the `AZURE_SUBSCRIPTION_ID`.
   4. Within the **Subscription details** page, you can find the **Subscription ID** in the top-right corner or in the general settings of the subscription.


---


Refer to [Azure Resource Manager - AzureDatabricks client library for Java](https://learn.microsoft.com/en-us/java/api/overview/azure/resourcemanager-databricks-readme?view=azure-java-preview) and [Azure Resource Manager AzureDatabricks client library for Java](https://github.com/Azure/azure-sdk-for-java/tree/main/sdk/resourcemanager) for further details.
