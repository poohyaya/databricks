# Databricks notebook source
# configure datalake source
configs = {"dfs.adls.oauth2.access.token.provider.type": "ClientCredential",
           "dfs.adls.oauth2.client.id": "<your-service-client-id>",
           "dfs.adls.oauth2.credential": "<your-service-credentials>",
           "dfs.adls.oauth2.refresh.url": "https://login.microsoftonline.com/<your-directory-id>/oauth2/token"}

dbutils.fs.mount(
  source = "adl://<your-data-lake-store-account-name>.azuredatalakestore.net/<your-directory-name>",
  mount_point = "/mnt/<mount-name>",
  extra_configs = configs)