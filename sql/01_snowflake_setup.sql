-- One-time setup in a Snowflake trial account. Run as ACCOUNTADMIN in a SQL worksheet.
USE ROLE ACCOUNTADMIN;

-- Cost guard: stop at 50 credits per month
CREATE OR REPLACE RESOURCE MONITOR recon_monitor
  WITH CREDIT_QUOTA = 50 FREQUENCY = MONTHLY START_TIMESTAMP = IMMEDIATELY
  TRIGGERS ON 80 PERCENT DO NOTIFY ON 100 PERCENT DO SUSPEND;

-- Smallest warehouse, suspends itself after 60 seconds
CREATE OR REPLACE WAREHOUSE recon_wh
  WAREHOUSE_SIZE = XSMALL AUTO_SUSPEND = 60 AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE RESOURCE_MONITOR = recon_monitor;

-- Database and layers
CREATE OR REPLACE DATABASE berlin_migration;
CREATE SCHEMA berlin_migration.raw;
CREATE SCHEMA berlin_migration.staging;
CREATE SCHEMA berlin_migration.marts;
CREATE SCHEMA berlin_migration.recon;

-- Role with only the rights the scripts need
CREATE OR REPLACE ROLE recon_role;
GRANT USAGE ON WAREHOUSE recon_wh TO ROLE recon_role;
GRANT ALL ON DATABASE berlin_migration TO ROLE recon_role;
GRANT ALL ON ALL SCHEMAS IN DATABASE berlin_migration TO ROLE recon_role;
GRANT ROLE recon_role TO ROLE ACCOUNTADMIN;

-- Technical user for the scripts: key-pair login, no password.
-- Generate a key pair first (see README) and paste the PUBLIC key (one line, without BEGIN/END lines).
CREATE OR REPLACE USER recon_loader
  TYPE = SERVICE
  DEFAULT_ROLE = recon_role
  DEFAULT_WAREHOUSE = recon_wh
  RSA_PUBLIC_KEY = '<YOUR_PUBLIC_KEY>';
GRANT ROLE recon_role TO USER recon_loader;

SELECT CURRENT_REGION();
