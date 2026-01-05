/*
This query tags encounters using abridge or not through ambient session and Smartdata element usage. Then we pull count information for all encounters and show a percentage of ambient usage by encounter and vsiit type.
*/

WITH
-- date parameters
DATE_PARAMS AS (
  SELECT
    CAST('2025-11-01' AS date) AS START_DATE,
    CAST('2025-12-22' AS date)    AS END_DATE
),
-- filter encounters to date
date_filtered_enc AS (
  SELECT e.PAT_ENC_CSN_ID, e.CONTACT_DATE
  FROM PAT_ENC e
  CROSS JOIN DATE_PARAMS dp
  WHERE e.CONTACT_DATE >= dp.START_DATE
    AND e.CONTACT_DATE <= dp.END_DATE
    AND e.ENC_TYPE_C IN (3,76,101) -- hospital, telemedicine, and office visit
    AND (e.APPT_STATUS_C IN (2,6) OR e.APPT_STATUS_C IS NULL) -- checking for completed or arrived or null
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
      AND sde.ELEMENT_ID IN ('EPIC#31000231848','EPIC#31000231852','EPIC#31000231850','EPIC#31000231851','EPIC#31000231856')-- these are the standard smartdata element values for Abridge
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

--ambient via note attribution
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

-- ambient from dxr
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

-- 3) single ambient flag
all_ambient AS (
  SELECT PAT_ENC_CSN_ID FROM ambient_via_nas
  UNION
  SELECT PAT_ENC_CSN_ID FROM ambient_via_sde
  UNION
  SELECT PAT_ENC_CSN_ID FROM ambient_via_attr
  UNION
  SELECT PAT_ENC_CSN_ID FROM ambient_via_dxr
),

-- grab the encounter info we care about
ENCOUNTER_INFO AS (
  SELECT
    penc.CONTACT_DATE AS encounter_date,
    penc.PAT_ENC_CSN_ID AS encounter_csn,
    penc.ENC_TYPE_C AS encounter_type_code,
    zenc.NAME AS encounter_type_name,
    penc.APPT_PRC_ID AS visit_type_code,
    prc.PRC_NAME AS visit_type_name,
    penc.VISIT_PROV_ID AS prov_id,
    ser.PROV_NAME AS prov_name,
    dep.DEPARTMENT_NAME AS department_name,
    dep.SPECIALTY AS department_specialty,
    penc.CHECKIN_TIME AS start_time, --using time of appt checking - could adjust to use EFFECTIVE_DATE_DTTM but we were seeing a lot of 12AM times showing.
    penc.ENC_CLOSE_TIME AS close_time,
    DATEDIFF(HOUR, penc.CHECKIN_TIME, penc.ENC_CLOSE_TIME) AS closure_hours,
    CASE WHEN aa.PAT_ENC_CSN_ID IS NOT NULL THEN 1 ELSE 0 END AS abridge_flag
  FROM PAT_ENC penc
  INNER JOIN date_filtered_enc dfe
          ON dfe.PAT_ENC_CSN_ID = penc.PAT_ENC_CSN_ID
  LEFT JOIN all_ambient aa
         ON aa.PAT_ENC_CSN_ID = penc.PAT_ENC_CSN_ID
  LEFT JOIN ZC_DISP_ENC_TYPE zenc
         ON penc.ENC_TYPE_C = zenc.DISP_ENC_TYPE_C
  LEFT JOIN CLARITY_PRC prc
         ON penc.APPT_PRC_ID = prc.PRC_ID
  LEFT JOIN CLARITY_SER ser
         ON ser.PROV_ID = penc.VISIT_PROV_ID
  LEFT JOIN CLARITY_DEP dep
         ON penc.DEPARTMENT_ID = dep.DEPARTMENT_ID
  WHERE penc.APPT_STATUS_C IN (2, 6) -- add additional filtering as needed to narrow encounter and visit types for site specific reporting
)
-- Individual encounter details with ambient detection methods
SELECT
  ei.encounter_date,
  ei.encounter_csn,
  ei.encounter_type_code,
  ei.encounter_type_name,
  ei.visit_type_code,
  ei.visit_type_name,
  ei.prov_id,
  ei.prov_name,
  ei.department_name,
  ei.department_specialty,
  ei.start_time,
  ei.close_time,
  ei.closure_hours,
  -- Overall ambient flag
  ei.abridge_flag,
  -- Which ambient CTE methods detected this encounter
  CASE WHEN sde.PAT_ENC_CSN_ID IS NOT NULL THEN 1 ELSE 0 END AS detected_via_sde,
  CASE WHEN nas.PAT_ENC_CSN_ID IS NOT NULL THEN 1 ELSE 0 END AS detected_via_nas,
  CASE WHEN attr.PAT_ENC_CSN_ID IS NOT NULL THEN 1 ELSE 0 END AS detected_via_attr,
  CASE WHEN dxr.PAT_ENC_CSN_ID IS NOT NULL THEN 1 ELSE 0 END AS detected_via_dxr,
  -- Concatenated list of detection methods
  CONCAT(
    CASE WHEN sde.PAT_ENC_CSN_ID IS NOT NULL THEN 'SDE, ' ELSE '' END,
    CASE WHEN nas.PAT_ENC_CSN_ID IS NOT NULL THEN 'NAS, ' ELSE '' END,
    CASE WHEN attr.PAT_ENC_CSN_ID IS NOT NULL THEN 'ATTR, ' ELSE '' END,
    CASE WHEN dxr.PAT_ENC_CSN_ID IS NOT NULL THEN 'DXR, ' ELSE '' END,
    ''
  ) AS ambient_detection_methods
FROM ENCOUNTER_INFO ei
LEFT JOIN ambient_via_sde sde ON sde.PAT_ENC_CSN_ID = ei.encounter_csn
LEFT JOIN ambient_via_nas nas ON nas.PAT_ENC_CSN_ID = ei.encounter_csn
LEFT JOIN ambient_via_attr attr ON attr.PAT_ENC_CSN_ID = ei.encounter_csn
LEFT JOIN ambient_via_dxr dxr ON dxr.PAT_ENC_CSN_ID = ei.encounter_csn
ORDER BY
  ei.encounter_date,
  ei.encounter_csn;

/* As a reminder, any information Abridge shares with you related to our internal processes, 
including our Epic scripts and queries, is considered confidential information under the services agreement between your health system and Abridge. 
Accordingly, Abridge's information generally may not be shared with third parties. */
