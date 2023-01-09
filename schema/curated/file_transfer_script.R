# Packages
library(RPostgres)
library(tidyverse)
library(RCurl)

# Connection
conn <- dbConnect(Postgres(),
                  dbname = Sys.getenv("DBNAME"),
                  host = Sys.getenv("HOST"),
                  port = Sys.getenv("PORT"),
                  user = Sys.getenv("USER"),
                  password = Sys.getenv("PASSWORD"),
                  timezone = "America/Los_Angeles")


# Tables
# table_names <- c(
#   "child_referral_episode",
#   "child_removal_episode",
#   "child_removal_supervision_level",
#   "sprout_provider_county_lookup",
#   "sprout_provider_office_lookup",
#   "sprout_provider_region_lookup",
#   "sprout_providers",
#   "unusual_incident_report_dcyf",
#   "visitation_referral",
#   "visitation_referral_participant",
#   "visitation_referral_provider",
#   "visit_report_dcyf",
#   "visit_report_participant",
#   "visitation_referral_action_log",
#   "unusual_incident_report_actions_dcyf",
#   "unusual_incident_report_participant",
#   "transport_detail"
#   )

table_names <- c("visit_report_billing_data",
                 "referral_intake_billing_data",
                 "ui_report_billing_data")

# Function to query db
extract_df <- function(tbl_name) {
  sql_from_string <- paste0("Select * FROM dcyf.", tbl_name)
  
  df <- tbl(conn, sql(sql_from_string)) %>%
    as_tibble()
  
  df
  
}

# Set local directory
date_path <- paste0("/Users/jooreea/OneDrive - UW/General/Data Team/Sprout DCYF OIAA Data/Transfer Files/", Sys.Date())

if(dir.exists(date_path) == FALSE) {
  
  dir.create(date_path)
  
}

row_counts <- c()
timestamps <- as.POSIXct(c())

# Loop to execute db query and write and transfer file
for(i in table_names) {
  
  df <- extract_df(i)
  
  row_counts <- c(row_counts, nrow(df))
  timestamps <- c(timestamps, Sys.time())
  
  local_path <- paste0(date_path, "/", i, ".csv")
  
  write.csv(df, local_path)
  
  ftp_path <- paste0("sftp://", mft_credentials, "@mft.wa.gov/sprout/", i, ".csv")

  ftpUpload(local_path,
            ftp_path, ftp.ssl = TRUE, ssl.verifypeer = FALSE, ssl.verifyhost = FALSE)
}

transfer_summary <- bind_cols(table = table_names, count = row_counts, timestamp = as.POSIXct(timestamps, origin = "1970-01-01"))
local_path <- paste0(date_path, "/transfer_summary", ".csv")

write.csv(transfer_summary, local_path)

ftp_path <- paste0("sftp://", mft_credentials,"@mft.wa.gov/sprout/transfer_summary.csv")

ftpUpload(local_path,
          ftp_path, ftp.ssl = TRUE, ssl.verifypeer = FALSE, ssl.verifyhost = FALSE)
