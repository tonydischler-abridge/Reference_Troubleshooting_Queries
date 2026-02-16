/*
This query gets the cpt, wRVU, and LOS from the system when available to create ENCOUNTER-LEVEL details of ambient and non-ambient encounters.
Many times the wRVU and LOS values from the system will be null which will require mapping from the CPT over to LOS and wRVU.

MODIFIED: Returns one row per encounter (PAT_ENC_CSN_ID) with all details instead of daily aggregates.
*/
WITH
-- date parameters
DATE_PARAMS AS (
  SELECT
    CAST('2024-01-01' AS date) AS START_DATE,
    CAST(GETDATE() AS date)    AS END_DATE
),

-- encounters filtered to date window. also filtering encounter type and status
date_filtered_enc AS (
  SELECT
	e.PAT_ENC_CSN_ID,
	e.CONTACT_DATE
  FROM PAT_ENC e
  CROSS JOIN DATE_PARAMS dp
  WHERE e.CONTACT_DATE >= dp.START_DATE
    AND e.CONTACT_DATE <= dp.END_DATE
    AND e.ENC_TYPE_C IN (3,76,101) -- hospital, telemedicine, and office visit -- update as needed based on where the system is being utilized
    AND (e.APPT_STATUS_C IN (2,6) OR e.APPT_STATUS_C IS NULL) -- checking for completed or arrived or null for Hopsital Encounters that don't get appointment statuses
),

-- which encounters touched SDE
ambient_via_sde AS (
  SELECT e.PAT_ENC_CSN_ID
  FROM date_filtered_enc e
  WHERE EXISTS (
    SELECT 1
    FROM SMRTDTA_ELEM_DATA sde
    INNER JOIN SMRTDTA_ELEM_VALUE sev
      ON sev.HLV_ID = sde.HLV_ID
    WHERE sde.CONTACT_SERIAL_NUM = e.PAT_ENC_CSN_ID
      AND sde.ELEMENT_ID IN ('EPIC#31000231848','EPIC#31000231852','EPIC#31000231850','EPIC#31000231851','EPIC#31000231856') -- these are the standard SDE values for Abridge or Ambient. Update if your site uses different smartdata elements
  )
),

-- which encounters had an ambient session
ambient_via_nas AS (
  SELECT e.PAT_ENC_CSN_ID
  FROM date_filtered_enc e
  WHERE EXISTS (
    SELECT 1
    FROM HNO_INFO hno
    INNER JOIN NOTE_AMBIENT_SECTIONS nas
      ON nas.NOTE_ID = hno.NOTE_ID
    WHERE hno.PAT_ENC_CSN_ID = e.PAT_ENC_CSN_ID
      AND nas.AMBIENT_SESSION_IDENT IS NOT NULL
  )
),

-- ambient via note attribution
ambient_via_attr AS (
  SELECT e.PAT_ENC_CSN_ID
  FROM date_filtered_enc e
  WHERE EXISTS (
    SELECT 1
    FROM HNO_INFO hno
    INNER JOIN NOTE_ATTRIBUTION na
      ON na.NOTE_ID = hno.NOTE_ID
    WHERE hno.PAT_ENC_CSN_ID = e.PAT_ENC_CSN_ID
      AND na.NOTEATTR_SOURCE_C = 25
  )
),

-- ambient via DXR
ambient_via_dxr AS (
  SELECT e.PAT_ENC_CSN_ID
  FROM date_filtered_enc e
  WHERE EXISTS (
    SELECT 1
    FROM DOCS_RCVD_DETAILS d
    INNER JOIN DOCS_RCVD_NOTE_SECTIONS n
      ON n.DOCUMENT_ID = d.DOCUMENT_ID
    WHERE d.DOCUMENT_RQST_CSN = e.PAT_ENC_CSN_ID
      AND d.DOCUMENT_RQST_CSN IS NOT NULL
  )
),

-- unified ambient list (standard)
all_ambient AS (
  SELECT PAT_ENC_CSN_ID FROM ambient_via_nas
  UNION
  SELECT PAT_ENC_CSN_ID FROM ambient_via_sde
  UNION
  SELECT PAT_ENC_CSN_ID FROM ambient_via_attr
  UNION
  SELECT PAT_ENC_CSN_ID FROM ambient_via_dxr
),

BaseEncounters AS (
    SELECT
        CAST(appt.CONTACT_DATE AS DATE) AS encounter_date,
        enc.PAT_ENC_CSN_ID, -- patient csn to link to encounter file
        ser.PROV_NAME,
        ser.PROV_ID,
        map.CID AS PROV_CID, -- provider cid, this should be commented out or replaced with custom item if partner is not licensed for intraconnect
        ser2.NPI, -- provider NPI
        enc.ENC_TYPE_C AS encounter_type_code,
        enct.NAME AS encounter_type_name, -- encounter type
        enc.APPT_PRC_ID AS prc_id, -- visit type
        dep.SPECIALTY AS specialty,
        pos.POS_TYPE_C AS place_of_service,
        enc.LOS_PROC_CODE AS cpt_code,
        eap.PROC_NAME AS cpt_description,
        CASE -- add additional codes to this case statement as needed to map from CMS file to wRVU
            WHEN TRY_CAST(enc.LOS_PROC_CODE AS INT) = 99211 THEN 0.18
            WHEN TRY_CAST(enc.LOS_PROC_CODE AS INT) = 99212 THEN 0.70
            WHEN TRY_CAST(enc.LOS_PROC_CODE AS INT) = 99213 THEN 1.30
            WHEN TRY_CAST(enc.LOS_PROC_CODE AS INT) = 99214 THEN 1.92
            WHEN TRY_CAST(enc.LOS_PROC_CODE AS INT) = 99215 THEN 2.80
            WHEN TRY_CAST(enc.LOS_PROC_CODE AS INT) = 99202 THEN 0.93
            WHEN TRY_CAST(enc.LOS_PROC_CODE AS INT) = 99203 THEN 1.60
            WHEN TRY_CAST(enc.LOS_PROC_CODE AS INT) = 99204 THEN 2.60
            WHEN TRY_CAST(enc.LOS_PROC_CODE AS INT) = 99205 THEN 3.50
            ELSE 0.0
        END AS mapped_wRVU,
        los.CALCULATED_LOS,
        CASE WHEN aa.PAT_ENC_CSN_ID IS NOT NULL THEN 'Ambient' ELSE 'Non-Ambient' END AS ambient_flag -- based on note attribution
    FROM V_PAT_ENC enc
    INNER JOIN date_filtered_enc dfe
        ON dfe.PAT_ENC_CSN_ID = enc.PAT_ENC_CSN_ID
    INNER JOIN F_SCHED_APPT appt
        ON appt.PAT_ENC_CSN_ID = enc.PAT_ENC_CSN_ID
    LEFT JOIN CLARITY_SER ser
        ON appt.PROV_ID = ser.PROV_ID
    LEFT JOIN SER_MAP map --not required, remove if not licensed for intraconnect
        ON map.INTERNAL_ID = ser.PROV_ID
    LEFT JOIN CLARITY_SER_2 ser2
        ON ser2.PROV_ID = ser.PROV_ID
    LEFT JOIN CLARITY_PRC prc
        ON prc.PRC_ID = appt.PRC_ID
    LEFT JOIN CLARITY_DEP dep
        ON enc.DEPARTMENT_ID = dep.DEPARTMENT_ID
    LEFT JOIN CLARITY_POS pos
        ON dep.REV_LOC_ID = pos.POS_ID
    LEFT JOIN CLARITY_EAP eap
        ON enc.LOS_PRIME_PROC_ID = eap.PROC_ID
    LEFT JOIN V_PAT_ENC_CALCULATED_LOS los
        ON los.PAT_ENC_CSN_ID = enc.PAT_ENC_CSN_ID
    LEFT JOIN CLARITY_UCL ucl
        ON ucl.EPT_CSN = enc.PAT_ENC_CSN_ID
    LEFT JOIN ZC_DISP_ENC_TYPE enct
        ON enc.ENC_TYPE_C = enct.DISP_ENC_TYPE_C
    LEFT JOIN NOTE_ENC_INFO info
        ON info.NOTE_ID =
            (SELECT TOP 1 hno.NOTE_ID
            FROM HNO_INFO hno
            WHERE hno.PAT_ENC_CSN_ID = enc.PAT_ENC_CSN_ID)
    LEFT JOIN all_ambient aa
        ON aa.PAT_ENC_CSN_ID = enc.PAT_ENC_CSN_ID
),
TransactionRVUs AS (
    SELECT
        t.PAT_ENC_CSN_ID,
        SUM(t.RVU_WORK) AS transaction_wRVU, -- pull the wRVU from the transaction if it exists-- this can be null many times which is why we map from the CPT
        MAX(p.POS_TYPE_C) AS pos_type_code
    FROM ARPB_TRANSACTIONS t
    INNER JOIN date_filtered_enc dfe
        ON dfe.PAT_ENC_CSN_ID = t.PAT_ENC_CSN_ID
    LEFT JOIN CLARITY_POS p
        ON t.POS_ID = p.POS_ID
    WHERE t.RVU_WORK IS NOT NULL
    GROUP BY t.PAT_ENC_CSN_ID
),
InitialDenial AS (
    SELECT
        hsp.PRIM_ENC_CSN_ID as PAT_ENC_CSN_ID,
        MAX(CASE WHEN bdc.IS_INITIAL_DENIAL_YN = 'Y' THEN 1 ELSE 0 END) AS initial_denial_flag
    FROM HSP_ACCOUNT hsp
    INNER JOIN date_filtered_enc dfe
        ON dfe.PAT_ENC_CSN_ID = hsp.PRIM_ENC_CSN_ID
	LEFT JOIN INVOICE i
		ON i.PB_HOSP_ACT_ID = hsp.HSP_ACCOUNT_ID
    LEFT JOIN BDC_INFO bdc
        ON bdc.PB_INVOICE_ID = i.INVOICE_ID
	GROUP By hsp.PRIM_ENC_CSN_ID
),

Combined AS (
    SELECT
        b.PROV_CID AS CID, -- remove if not intraconnect licensed
        b.NPI AS NPI,
        b.PROV_ID,
        b.PROV_NAME,
        b.encounter_date,
        b.encounter_type_code,
        b.encounter_type_name,
        b.prc_id,
        b.specialty,
        b.ambient_flag,
        b.place_of_service,
        b.cpt_code,
        b.cpt_description,
        b.mapped_wRVU,
        ar.transaction_wRVU,
        ar.pos_type_code,
        b.CALCULATED_LOS,
        b.PAT_ENC_CSN_ID,
        id.initial_denial_flag
    FROM BaseEncounters b
    LEFT JOIN TransactionRVUs ar
        ON b.PAT_ENC_CSN_ID = ar.PAT_ENC_CSN_ID
    LEFT JOIN InitialDenial id
        ON b.PAT_ENC_CSN_ID = id.PAT_ENC_CSN_ID
),

PerEncounter AS (
    SELECT
        PAT_ENC_CSN_ID,
        encounter_date,
        encounter_type_code,
        encounter_type_name,
        prc_id,
        NPI,
        PROV_ID,
        PROV_NAME,
        CID, -- remove if not using intraconnect
        specialty,
        ambient_flag,
        place_of_service,
        pos_type_code,
        cpt_code,
        cpt_description,
        MAX(COALESCE(mapped_wRVU, 0))            AS encounter_mapped_wRVU,
        MAX(COALESCE(transaction_wRVU, 0))       AS encounter_transaction_wRVU,
        MAX(COALESCE(CALCULATED_LOS, 0))         AS CALCULATED_LOS,
        MAX(COALESCE(initial_denial_flag, 0))    AS initial_denial_flag
    FROM Combined
    GROUP BY
        PAT_ENC_CSN_ID,
        encounter_date,
        encounter_type_code,
        encounter_type_name,
        prc_id,
        NPI,
        PROV_ID,
        PROV_NAME,
        CID, -- remove if not using intraconnect
        specialty,
        ambient_flag,
        place_of_service,
        pos_type_code,
        cpt_code,
        cpt_description
)

-- MODIFIED: Final SELECT returns encounter-level details instead of daily aggregates
SELECT
    PAT_ENC_CSN_ID,
    encounter_date,
    encounter_type_code,
    encounter_type_name,
    prc_id,
    NPI,
    PROV_ID,
    PROV_NAME,
    CID, -- remove if not using intraconnect
    specialty,
    ambient_flag,
    place_of_service,
    pos_type_code,
    cpt_code,
    cpt_description,
    encounter_mapped_wRVU,
    encounter_transaction_wRVU,
    CALCULATED_LOS,
    initial_denial_flag
FROM PerEncounter
ORDER BY encounter_date DESC, PAT_ENC_CSN_ID;

/* As a reminder, any information Abridge shares with you related to our internal processes,
including our Epic scripts and queries, is considered confidential information under the services agreement between your health system and Abridge.
Accordingly, Abridge's information generally may not be shared with third parties. */
