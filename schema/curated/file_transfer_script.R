# Packages
library(RPostgres)
library(tidyverse)
library(RCurl)

# Connection
conn <- dbConnect(RPostgres::Postgres(),
                  dbname = "db",
                  host = "localhost.aptible.in",
                  port = "64444",
                  user = "aptible",
                  password = "DrhkmT-ldghOrkxW71WLbJAQdRsP2WuQ",
                  timezone = "America/Los_Angeles")


# Tables
table_names <- c(
  "child_referral_episode",
  "child_removal_episode",
  "child_removal_supervision_level",
  "sprout_provider_county_lookup",
  "sprout_provider_office_lookup",
  "sprout_provider_region_lookup",
  "sprout_providers",
  "unusual_incident_report_dcyf",
  "visitation_referral",
  "visitation_referral_participant",
  "visitation_referral_provider",
  "visit_report_dcyf",
  "visit_report_participant",
  "visitation_referral_action_log",
  "unusual_incident_report_actions_dcyf",
  "unusual_incident_report_participant"
  )

# Function to query db
extract_df <- function(tbl_name) {
  sql_from_string <- paste0("Select * FROM dcyf.", tbl_name)
  
  df <- tbl(conn, sql(sql_from_string)) %>%
    as_tibble()
  
  df
  
}

# Set local directory
date_path <- paste0("/Volumes/GoogleDrive/Shared drives/cssat/Sprout Family Time Data Warehouse/cssat_dcyf_transfer_files/", Sys.Date())

if(dir.exists(date_path) == FALSE) {
  
  dir.create(date_path)
  
}

row_counts <- c()
timestamps1 <- as.POSIXct(c())

# Loop to execute db query and write and transfer file
for(i in table_names) {
  
  df <- extract_df(i)
  
  row_counts <- c(row_counts, nrow(df))
  timestamps <- c(timestamps, Sys.time())
  
  local_path <- paste0(date_path, "/", i, ".csv")
  
  write.csv(df, local_path)
  
  ftp_path <- paste0("ftp://dcyf-uw-jooree.ahn:Highland!1Denny!2@sft.wa.gov/sprout/", i, ".csv")
  
  ftpUpload(local_path,
            ftp_path, ftp.ssl = TRUE, ssl.verifypeer = FALSE, ssl.verifyhost = FALSE)
}

transfer_summary <- bind_cols(table = table_names, count = row_counts, timestamp = as.POSIXct(timestamps, origin = "1970-01-01"))
local_path <- paste0(date_path, "/transfer_summary", ".csv")

write.csv(transfer_summary, local_path)

ftp_path <- paste0("ftp://dcyf-uw-jooree.ahn:Highland!1Denny!2@sft.wa.gov/sprout/transfer_summary.csv")

ftpUpload(local_path,
          ftp_path, ftp.ssl = TRUE, ssl.verifypeer = FALSE, ssl.verifyhost = FALSE)
