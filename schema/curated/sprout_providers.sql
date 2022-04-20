-- View: dcyf.sprout_providers

-- DROP MATERIALIZED VIEW IF EXISTS dcyf.sprout_providers;

CREATE MATERIALIZED VIEW IF NOT EXISTS dcyf.sprout_providers
TABLESPACE pg_default
AS
 SELECT o.id AS id_provider_sprout,
    o.name AS provider_name,
    o."contactName" AS provider_contact_name,
    o.phone AS provider_contact_phone,
    o.email AS provider_contact_email,
    o."billingAddress" AS billing_address_line1,
    o.city AS billing_address_city,
    o.state AS billing_address_state,
    o."postalCode" AS billing_address_postal_code,
        CASE
            WHEN regexp_replace(split_part(o."famlinkId"::text, '/'::text, 1), '\D'::text, ''::text, 'g'::text) ~ '^([0-9]+)'::text THEN regexp_replace(split_part(o."famlinkId"::text, '/'::text, 1), '\D'::text, ''::text, 'g'::text)::integer
            ELSE NULL::integer
        END AS famlink_provider_id,
        CASE
            WHEN o."visitationOnSaturday" = true THEN 1
            WHEN o."visitationOnSaturday" = false THEN 0
            ELSE NULL::integer
        END AS fl_visitation_on_saturday,
        CASE
            WHEN o."visitationOnSunday" = true THEN 1
            WHEN o."visitationOnSunday" = false THEN 0
            ELSE NULL::integer
        END AS fl_visitation_on_sunday,
    o."bilingualSupportTypeId" AS bilingual_support_type_id,
        CASE
            WHEN o."bilingualSupportTypeId" = 1 THEN 'No language support offered'::text
            WHEN o."bilingualSupportTypeId" = 2 THEN 'No, use interpreter'::text
            WHEN o."bilingualSupportTypeId" = 3 THEN 'Yes, specifically these languages:'::text
            ELSE NULL::text
        END AS bilingual_support_type,
    o."languagesSupported" AS languages_supported,
    o."holidayAvailabilityTypeId" AS holiday_availibility_type_id,
        CASE
            WHEN o."holidayAvailabilityTypeId" = 1 THEN 'Open all holidays (normal hours)'::text
            WHEN o."holidayAvailabilityTypeId" = 2 THEN 'Open all holidays (modified hours)'::text
            WHEN o."holidayAvailabilityTypeId" = 3 THEN 'Open some holidays (call for details)'::text
            WHEN o."holidayAvailabilityTypeId" = 4 THEN 'Closed only on these holidays:'::text
            ELSE NULL::text
        END AS holiday_availibility_type,
    o."holidaysClosed" AS holiday_closed,
        CASE
            WHEN o.searchable = true THEN 1
            WHEN o.searchable = false THEN 0
            ELSE NULL::integer
        END AS fl_searchable,
        CASE
            WHEN o."routingOrg" = true THEN 1
            WHEN o."routingOrg" = false THEN 0
            ELSE NULL::integer
        END AS fl_routing_organization,
        CASE
            WHEN o."hasSocialWorkers" = true THEN 1
            WHEN o."hasSocialWorkers" = false THEN 0
            ELSE NULL::integer
        END AS fl_has_social_workers,
    o."createdAt" AS dt_create,
    o."updatedAt" AS dt_update,
    o."deletedAt" AS dt_deleted
   FROM dcyf.organizations o
  ORDER BY o.id
WITH NO DATA;

ALTER TABLE IF EXISTS dcyf.sprout_providers
    OWNER TO aptible;

GRANT ALL ON TABLE dcyf.sprout_providers TO aptible;
GRANT SELECT ON TABLE dcyf.sprout_providers TO dcyf_users;
