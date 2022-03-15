## This code was written in February/March 2022
## Updating Joe's original code to fill in gaps

## Assumes you have an .Renviron file set up defining 
# CANS_F_URL
# CANS_F_DB
# CANS_F_USER
# CANS_F_PW
## All of these should be viewable after running the following command lines:
#  aptible login --email gregort@uw.edu 
#  aptible db:tunnel cans-db-production --port 64453

## Where should the output go?
output_dir = "cans-output"

library(mongolite)
library(dplyr)
library(tidyr)
library(purrr)
library(readr)
library(stringr)
library(lubridate)

## command line connection works with (can use mongosh or mongo)
## mongo "mongodb://<user>:<password>@localhost.aptible.in:<port>/<db>?tls=true"
## in MongoDB compass, in More Options, set SSL to first non-None option

debug = FALSE ## set to true to examine data more

con = mongo(
  url = Sys.getenv("CANS_F_URL"),
  verbose = TRUE, ## consider switching off?
  options = ssl_options(weak_cert_validation = T)
)


## Testing
# referrals = mongo(
#   collection = "referrals", 
#   url = Sys.getenv("CANS_F_URL"),
#   verbose = TRUE, ## consider switching off
#   options = ssl_options(weak_cert_validation = T)
# )$find("{}")

## This seems to work
collection_to_tibble = function(collection) {
  mongo(
    collection = collection,
    url = Sys.getenv("CANS_F_URL"),
    verbose = TRUE,
    options = ssl_options(weak_cert_validation = T)
  )$find('{}')
}

## pull in raw data ####
cases = collection_to_tibble(
  collection = "cases"
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

## basic transformations ####

## removing deleted cases
status = data.frame(
  caseId = cases$caseId,
  status = cases$caseProfile$caseStatus
) %>%
  filter(status != "deleted")
## this looks good

cases = cases %>% filter(caseId %in% status$caseId)
referrals = referrals %>% filter(caseId %in% status$caseId) %>% distinct()
forms = forms %>% filter(caseId %in% status$caseId)
timestamps = timestamps %>% filter(caseId %in% status$caseId)


cans_providers = select(providers, 
  id_organization_sprout = providerId,
  name,
  phone,
  address
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
    tx_natural_resources = map_chr(Community, paste, collapse = "; "),
    tx_additional_supports = map_chr(Additional, paste, collapse = "; ")
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
    dt_trans, #Date of Trans CANS
    dt_eoi, #For now, the same as the date of submission
    int_caregiver_count = NumCaregivers,
    int_child_count = NumChildren,
    tx_reason_for_referral = ReasonForServiceReferral,
    tx_reason_for_eoi = ReasonForEOI, 
    tx_reason_for_not_completed = NotCompleted, #Description if service not needed
    tx_natural_resources, # Resources
    tx_additional_supports
  ) %>% 
  filter(
    id_organization_sprout != 1
  ) 

if(debug) {
  ## these are often the same. following Matt's advice we will
  ## prefer the direct $Assessments fa
  assessment = "ChildFunctioningList"
  fda = lapply(forms[["data"]][["Assessments"]], "[[", assessment)
  fa = lapply(forms$Assessments, "[[", assessment)
    
  for (i in sample(seq_along(fa), size = 20)) {
    print(i)
    print(identical(fda[[i]], fa[[i]]))
    print(is.null(fda[[i]]))
    cat("\n")
  }
}

assessment_extract_individual = function(forms, assessment) {
  #assessment = "ChildFunctioningList"
  fa = lapply(forms$Assessments, "[[", assessment)
  fadf = lapply(fa, bind_rows, .id = "assessment_sequence")
  names(fadf) = forms$caseId
  fadf = bind_rows(fadf, .id = "caseId") %>%
    mutate(type_of_assessment = assessment, .after = caseId) %>%
    unnest(Ratings) %>%
    rename(
      person_id = id,
      item_id = Id,
      id_case_sprout = caseId
    ) %>%
    mutate(Rating = if_else(Rating == -1, NA_integer_, Rating))
}



assessment_extract_family = function(forms, assessment) {
  #assessment = "FamilyFunctioningRatings"
  fa = lapply(forms$Assessments, "[[", assessment)
  fadf = lapply(fa, bind_rows, .id = "assessment_sequence")
  names(fadf) = forms$caseId
  fadf = bind_rows(fadf, .id = "caseId") %>%
    mutate(
      type_of_assessment = assessment, .after = caseId) %>%
    rename(
      item_id = Id,
      id_case_sprout = caseId
    ) %>%
    mutate(Rating = if_else(Rating == -1, NA_integer_, Rating))
}

## Reports output
output = list(
  cans_family_functioning = assessment_extract_family(forms, "FamilyFunctioningRatings"),
  cans_caregiver_advocacy_functioning = assessment_extract_family(forms, "CaregiverAdvocacyRatings"),
  cans_child_functioning = assessment_extract_individual(forms, "ChildFunctioningList"),
  cans_caregiver_functioning = assessment_extract_individual(forms, "CaregiverFunctioningList")
)

## Remove tabs from comment fields and move to last
for(i in seq_along(output)) {
  if("Comment" %in% names(output[[i]])) {
    output[[i]] = output[[i]] %>%
      relocate(Comment, .after = last_col()) %>%
      mutate(Comment = str_replace_all(Comment, pattern = "\\s+", " "))
  }
}

## Adding providers and common
output = c(output, cans_providers = list(cans_providers), cans_common = list(cans_common))

## Inspecting
if(debug) {
  lapply(output, \(x) View(head(x, 500)))
}


## pipe delimited preferred.

for(i in seq_along(output)) {
  write_delim(
    output[[i]],
    file = paste0(output_dir, "/", names(output)[i], ".csv"),
    delim = "|"
  )
}


