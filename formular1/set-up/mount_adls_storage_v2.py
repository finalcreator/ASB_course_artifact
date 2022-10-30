# Databricks notebook source
# MAGIC %md
# MAGIC #### Initialization

# COMMAND ----------

###############################################################################
# Create an Azure Service Principal and give it access to our storage account #
###############################################################################
# vedio 25,26,27
# step 1: Create Service Principal account
# - Goto Azure Active Directory > App Registration > New Registration: databricks-course-service-app
# > Copy the 'Application (client) ID' & 'Directory (tenant) ID' into a NOTEPAD
# - Click on Certificates & secrets > New client secret > Description: databricks-course-access-secret
# > Copy the secret value into a NOTEPAD
# step 2: Give storage account access to the Service Principal account
# - Goto Storage accounts > Select it: adfcourseanyistaccdl > Access Control (IAM) 
#   > Add role assignment > Storage Blob Data Contributor > User, group, or service principal > Select members 
#   > search for the Service Principal account you just created:databricks-course-service-app > select > create


####Crdentials are exposed hence we should use Azure Key vaults to keep our secrets
### Vedio 29



# COMMAND ----------

# MAGIC %md
# MAGIC ### Calling Key Vault Secrete

# COMMAND ----------

# - after creating key vault
#  > copy the client-id, tenant-id, client-secret into the key-vault
# - click on the databricks home page
# - append 'secrets/createScope' to the URL 'https://adb-7576904290643392.12.azuredatabricks.net/?o=7576904290643392#'
#   > https://adb-7576904290643392.12.azuredatabricks.net/?o=7576904290643392#secrets/createScope
# - this will open a form
# Fill into the form as follows
# * Scope Name: adfcourseanyistaccdl-scope
# * Manage Principal: Select > All Users (Select 'Creator' option if it is premium.)
# * DNS Name: Goto Key vaults > course-test-key-vault > properties > copy 'Vault URI'
# * Resource ID: Goto Key vaults > course-test-key-vault > properties > copy 'Resource ID'

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Use the dbutils secrets to manipulate the key vaults

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

# lists the seceret scopes to the sorage accounts
dbutils.secrets.listScopes()

# COMMAND ----------

# we can list the secerets in the 'adfcourseanyistaccdl-scope'
dbutils.secrets.list('adfcourseanyistaccdl-scope')

# COMMAND ----------

# We can also get the secerets like this
# Hence, we see that its tries to hide the secerets so that it does not show in the output
dbutils.secrets.get(scope="adfcourseanyistaccdl-scope", key="databricks-app-client-id")

# COMMAND ----------

# using this can also reviel secrets hence be careful who has access to your notebook
#for x in dbutils.secrets.get(scope="adfcourseanyistaccdl-scope", key="databricks-app-client-id"):
#    print(x)

# COMMAND ----------

## Now the secretes are no longer hard coded
storage_account_name = "adfcourseanyistaccdl"
client_id = dbutils.secrets.get(scope="adfcourseanyistaccdl-scope", key="databricks-app-client-id")
tenant_id = dbutils.secrets.get(scope="adfcourseanyistaccdl-scope", key="databricks-app-tenant-id")
client_secret = dbutils.secrets.get(scope="adfcourseanyistaccdl-scope", key="databricks-app-client-secret")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Setup Configurations

# COMMAND ----------

configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": f"{client_id}",
    "fs.azure.account.oauth2.client.secret": f"{client_secret}",
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
}

# COMMAND ----------

# MAGIC %md
# MAGIC #### Mount our storage

# COMMAND ----------

# function to mount the containers
def mount_adls(container_name):
    # mount the container
    dbutils.fs.mount(
        source = f'abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/',
        mount_point = f'/mnt/{storage_account_name}/{container_name}',
        extra_configs = configs
    )

# COMMAND ----------

raw_ctnr_name = "raw-ctnr"
proc_ctnr_name = "processed-ctnr"
pres_ctnr_name = "presentation-ctnr"
demo_ctnr_name = "demo-ctnr"

# COMMAND ----------

mount_adls(raw_ctnr_name)


# COMMAND ----------

mount_adls(proc_ctnr_name)

# COMMAND ----------

mount_adls(pres_ctnr_name)

# COMMAND ----------

mount_adls(demo_ctnr_name)

# COMMAND ----------

# MAGIC %md
# MAGIC #### List all files in our mount

# COMMAND ----------

# list the files in our mount
# /mnt/adfcourseanyistaccdl/raw-ctnr
dbutils.fs.ls("/mnt/{}/{}".format(storage_account_name, raw_ctnr_name))

# COMMAND ----------

dbutils.fs.ls("/mnt/{}/{}".format(storage_account_name, proc_ctnr_name))

# COMMAND ----------

dbutils.fs.ls("/mnt/{}/{}".format(storage_account_name, pres_ctnr_name))

# COMMAND ----------

dbutils.fs.ls("/mnt/{}/{}".format(storage_account_name, demo_ctnr_name))

# COMMAND ----------

## you can unmount containers if you want
#dbutils.fs.unmount("/mnt/{}/{}".format(storage_account_name, raw_ctnr_name))
#dbutils.fs.unmount("/mnt/{}/{}".format(storage_account_name, proc_ctnr_name))
#dbutils.fs.unmount("/mnt/{}/{}".format(storage_account_name, pres_ctnr_name))
#dbutils.fs.unmount("/mnt/{}/{}".format(storage_account_name, demo_ctnr_name))

# COMMAND ----------

# MAGIC %md
# MAGIC #### List all the mounts in our workspace

# COMMAND ----------

# list all the mounts in our workspace
display(dbutils.fs.mounts())

# COMMAND ----------


