CREATE MATERIALIZED VIEW IF NOT EXISTS curated."ServiceReferrals_mv" AS

SELECT  
    sr."versionId",
    sr."organizationId",
    sr."createdAt",
    sr."updatedAt",
    sr."updatedById",
    sr."notes",
    sr."referralId",
    sr."socialWorkerLastName",
    sr."socialWorkerFirstName",
    sr."socialWorkerPhone",
    sr."socialWorkerEmail",
    sr."dshsOffice",
    sr."dshsOfficePhone",
    sr."dcfsUnit",
    sr."caseSupervisorLastName",
    sr."caseSupervisorFirstName",
    sr."caseSupervisorPhone",
    sr."serviceType",
    sr."levelOfSupervision",
    sr."caseNumber",
    sr."caseDetailsLastName",
    sr."caseDetailsFirstName",
    sr."fosterParents",
    sr."relatives",
    sr."caseAidesOrIntern",
    sr."other",
    sr."barriersToConsideredAlternatives",
    sr."visitsPerWeek",
    sr."hoursPerVisit",
    sr."timeNegotiable",
    sr."providerContactLastName",
    sr."providerContactFirstName",
    sr."providerContactPhone",
    sr."restroomRules",
    sr."languageRequirementNeed",
    sr."language",
    sr."languageSpecification",
    sr."careSpecificInstructionsComment",
    sr."relatedCases",
    sr."emergencyNumbers",
    sr."childDetails",
    sr."parentVisitorDetails",
    sr."visitLocations",
    sr."visitSchedule",
    sr."safetyIssues",
    sr."childPlacements",
    sr."pickupLocations",
    sr."dropoffLocations",
    sr."id",
    sr."isCurrentVersion",
    sr."deletedAt",
    sr."inReviewReportsCount",
    sr."inProgressReportsCount",
    sr."intakeTotalTime",
    sr."referralState",
    sr."striveStatusId",
    sr."famlinkIdCache",
    sr."inReviewUnusualIncidentReportsCount",
    sr."inProgressUnusualIncidentReportsCount",
    sr."formVersion",
    sr."visitProviderType",
    sr."visitMethodType",
    sr."visitTransportationType",
    sr."visitOtherPreferredProvider",
    sr."visitPreferredProviderId",
    sr."visitPlanId",
    sr."levelOfSupervisionExplanation",
    sr."locations",
    sr."childPlacingAgency",
    sr."parentGuardianDetails",
    sr."approvedVisitorDetails",
    sr."overnightVisitsApprovedDate",
    sr."referralReason",
    sr."eisCaseName",
    sr."importVersion",
    sr."versionCreatedAt",
    sr."intakeDateCompleted",
    sr."requestDate",
    sr."startDate",
    sr."furthestPage",
    sr."initialVisitPlanId",
    sr."isRestricted",
    sr."endDate",
    sr."visitFrequency",
    sr."visitFrequencyUnit",
    sr."socialWorkerId",
    sr."regionId",
    sr."caseSupervisorId",
    sr."visitPlanApprovedDate",
    sr."routingOrganizationId",
    sr."safetyIssuesExplanation",
    sr."caseSupervisorEmail",
    sr."visitPlanName",
    sr."timeNegotiableNotes",
    sr."courtOrderNotes",
    sr."dshsOfficeFax",
    sr."formVersion" = 'Ingested' AS "isIngested",
    sr."serviceType" = 'Sibling' AS "isSiblingVisit",
    sr."serviceType" = 'Parent / Child' AS "isParentChildVisit",
    sr."serviceType" = 'Transportation Only' AS "isTransportationOnlyVisit",
    sr."serviceType" = 'Parent / Child with Transportation' AS "isParentChildVisitWithTransport",
    sr."serviceType" = 'Sibling with Transportation' AS "isSiblingVisitWithTransport",
	ROW_NUMBER() OVER (
        PARTITION BY sr.id 
        ORDER BY sr."versionId"
    ) AS "serviceReferralVersionRank",
    now() "viewRefreshedAt"
FROM sprout."ServiceReferrals" sr
WHERE sr."organizationId" IN (SELECT id FROM curated."Organizations_mv");

CREATE UNIQUE INDEX IF NOT EXISTS "ServiceReferrals_mv_pkey" 
    ON curated."ServiceReferrals_mv" USING btree ("versionId");
CREATE INDEX IF NOT EXISTS service_referrals_view_case_details_first_name 
    ON curated."ServiceReferrals_mv" USING btree ("caseDetailsFirstName");
CREATE INDEX IF NOT EXISTS service_referrals_view_case_details_last_name 
    ON curated."ServiceReferrals_mv" USING btree ("caseDetailsLastName");
CREATE INDEX IF NOT EXISTS service_referrals_view_created_at 
    ON curated."ServiceReferrals_mv" USING btree ("createdAt");
CREATE INDEX IF NOT EXISTS service_referrals_view_current_version_organization_id 
    ON curated."ServiceReferrals_mv" USING btree ("organizationId") 
    WHERE (("isCurrentVersion" = true) 
        AND ("deletedAt" IS NULL));
CREATE INDEX IF NOT EXISTS service_referrals_view_deleted_at 
    ON curated."ServiceReferrals_mv" USING btree ("deletedAt");
CREATE INDEX IF NOT EXISTS service_referrals_view_famlink_id_cache 
    ON curated."ServiceReferrals_mv" USING btree ("famlinkIdCache");
CREATE INDEX IF NOT EXISTS service_referrals_view_id 
    ON curated."ServiceReferrals_mv" USING btree (id);
CREATE INDEX IF NOT EXISTS service_referrals_view_is_current_version 
    ON curated."ServiceReferrals_mv" USING btree ("isCurrentVersion");
CREATE INDEX IF NOT EXISTS service_referrals_view_organization_id 
    ON curated."ServiceReferrals_mv" USING btree ("organizationId");
CREATE INDEX IF NOT EXISTS service_referrals_view_visit_plan_id 
    ON curated."ServiceReferrals_mv" USING btree ("visitPlanId");
CREATE INDEX IF NOT EXISTS service_referrals_view_routing_organization_id 
    ON curated."ServiceReferrals_mv" USING btree ("routingOrganizationId");

    