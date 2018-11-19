# Databricks notebook source
jdbcHostname = "tcp://kothkimpaasdb1.windows.databaes.net"
jdbcDatabase = "AdventureWorksLT"
username = "pfesql"
password = "Password1!@#"
jdbcPort = 1433
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2};user={3};password={4}".format(jdbcHostname, jdbcPort, jdbcDatabase, username, password)
