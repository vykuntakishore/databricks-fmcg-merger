# Databricks notebook source
%sql

CREATE CATALOG IF NOT EXISTS fmcg;
USE CATALOG fmcg


%sql
CREATE SCHEMA IF NOT EXISTS fmcg.gold;
CREATE SCHEMA IF NOT EXISTS fmcg.silver;
CREATE SCHEMA IF NOT EXISTS fmcg.bronze;



# DBTITLE 1,Cell 3
%sql

select count(*) from fmcg.gold.fact_orders;