## This was the code Joe could find for the original run in early 2021.
## It missing the Mongo connection, and utility function definitions.
## DCYF also raised several issues with the file.

library(mongolite)
library(dplyr)
library(tidyr)

con <- DBI::dbConnect(
  RPostgres::Postgres(),
  host = "localhost",
  dbname = Sys.getenv("DCYF_CSSAT_REPLICA_NAME"),
  user = Sys.getenv("DCYF_CSSAT_INSTANCE_MASTER"),
  password = Sys.getenv("DCYF_CSSAT_INSTANCE_PASSWORD"),
  port = 5433
)

referrals <- collection_to_tibble(
  collection = "referrals"
)

forms <- collection_to_tibble(
  collection = "cansforms"
)

providers <- collection_to_tibble(
  collection = "providers"
)

timestamps <- collection_to_tibble(
  collection = "timestamps"
)

facesheets <- forms %>% 
  .$FaceSheet %>%
  as_tibble()

eois <- bind_cols(
  caseId = forms$caseId,
  forms %>% 
    .$EndOfIntervention %>%
    as_tibble()
)

cans_common <- referrals %>%
  left_join(
    forms,
    by = c("caseId")
  ) %>%
  left_join(
    timestamps %>%
      select(
        caseId,
        assignment1,
        assignment2,
        assignment3
      ) %>%
      group_by(
        caseId
      ) %>%
      summarise(
        assignment1 = max(assignment1),
        assignment2 = max(assignment2),
        assignment3 = max(assignment3)
      ),
    by = c("caseId")
  ) %>%
  left_join(
    facesheets,
    by = c("caseId" = "CaseID")
  ) %>% 
  left_join(
    eois,
    by = c("caseId")
  ) %>%
  mutate(
    dt_cans_submitted = NA,
    tx_case_name = ifelse(Family=="", NA, tolower(Family)),
    tx_dcyf_office = ifelse(Office=="", NA, tolower(Office)),
    dt_referral_received = lubridate::date(DateReferral),
    dt_iff = lubridate::date(DateFirstMeeting), 
    epoch_fpc = ifelse(assignment1 == -1, NA, assignment1),
    dt_fpc = lubridate::as_datetime(as.POSIXct(epoch_fpc/1000, origin="1970-01-01")),
    epoch_trans = ifelse(assignment2 == -1, NA, assignment2),
    dt_trans = lubridate::as_datetime(as.POSIXct(epoch_trans/1000, origin="1970-01-01")),
    epoch_eoi = ifelse(assignment3 == -1, NA, assignment3),
    dt_eoi = lubridate::as_datetime(as.POSIXct(epoch_eoi/1000, origin="1970-01-01")),
    tx_resources = NA,
    tx_services = NA
  ) %>%
  select(
    id_case_sprout = caseId, #Sprout UUID
    id_organization_sprout = providerId, #Sprout Provider Id
    dt_cans_submitted, #Date of submission
    id_cihs_referral = ReferralID, #Placeholder, CaseId is not collected. prob synonymous 
    tx_case_name, #Source of Family Name
    tx_dcyf_office, #Office serving case
    tx_therapist_organization = Agency,
    tx_therapist = Provider, # The therapist
    tx_service_code = ServiceType,
    dt_referral_received, #Date referral received 
    dt_iff, #Date iff
    dt_fpc, #Date of Family Plan for Change
    dt_trans, #Date of Trans CANSs
    dt_eoi, #For now, the same as the date of submission
    int_caregiver_count = NumCaregivers,
    int_child_count = NumChildren,
    tx_reason_for_referral = ReasonForServiceReferral,
    tx_reason_for_eoi = ReasonForEOI, 
    tx_reason_for_not_completed = NotCompleted, #Description if service not needed
    tx_resources, # Resources
    tx_services
  ) %>% 
  filter(
    id_organization_sprout != 1
  ) 

cans_child_functioning <- assessment_extract_individual(forms, "ChildFunctioningList") %>%
  inner_join(
    cans_common,
    by = "id_case_sprout"
  )

cans_caregiver_functioning <- assessment_extract_individual(forms, "CaregiverFunctioningList") %>%
  inner_join(
    cans_common,
    by = "id_case_sprout"
  )

cans_family_functioning <- assessment_extract_family(forms, "FamilyFunctioningRatings") %>%
  inner_join(
    cans_common,
    by = "id_case_sprout"
  )

cans_caregiver_advocacy_functioning <- assessment_extract_family(forms, "CaregiverAdvocacyRatings") %>%
  inner_join(
    cans_common,
    by = "id_case_sprout"
  )

DBI::dbWriteTable(
  conn = con, 
  name = DBI::SQL("staging.cans_common"),
  value = cans_common
)

DBI::dbWriteTable(
  conn = con, 
  name = DBI::SQL("staging.cans_child_functioning"),
  value = cans_child_functioning
)

DBI::dbWriteTable(
  conn = con, 
  name = DBI::SQL("staging.cans_caregiver_functioning"),
  value = cans_caregiver_functioning
)

DBI::dbWriteTable(
  conn = con, 
  name = DBI::SQL("staging.cans_family_functioning"),
  value = cans_family_functioning
)

DBI::dbWriteTable(
  conn = con, 
  name = DBI::SQL("staging.cans_caregiver_advocacy_functioning"),
  value = cans_caregiver_advocacy_functioning
)