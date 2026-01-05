/*
This query provides a count of Ambient vs Non-Ambient encounters by encounter type.
No encounter type filtering applied to capture all encounter types.
*/

WITH
-- date parameters
DATE_PARAMS AS (
  SELECT
    CAST('2025-11-01' AS date) AS START_DATE,
    CAST('2025-12-22' AS date) AS END_DATE
),
-- filter encounters to date (no encounter type filter)
date_filtered_enc AS (
  SELECT e.PAT_ENC_CSN_ID, e.CONTACT_DATE
  FROM PAT_ENC e
  CROSS JOIN DATE_PARAMS dp
  WHERE e.CONTACT_DATE >= dp.START_DATE
    AND e.CONTACT_DATE <= dp.END_DATE
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

-- single ambient flag
all_ambient AS (
  SELECT PAT_ENC_CSN_ID FROM ambient_via_nas
  UNION
  SELECT PAT_ENC_CSN_ID FROM ambient_via_sde
  UNION
  SELECT PAT_ENC_CSN_ID FROM ambient_via_attr
  UNION
  SELECT PAT_ENC_CSN_ID FROM ambient_via_dxr
)

-- Count by encounter type
SELECT
  penc.ENC_TYPE_C AS encounter_type_code,
  zenc.NAME AS encounter_type_name,
  COUNT(DISTINCT penc.PAT_ENC_CSN_ID) AS total_encounters,
  SUM(CASE WHEN aa.PAT_ENC_CSN_ID IS NOT NULL THEN 1 ELSE 0 END) AS ambient_encounters,
  SUM(CASE WHEN aa.PAT_ENC_CSN_ID IS NULL THEN 1 ELSE 0 END) AS non_ambient_encounters,
  CAST(SUM(CASE WHEN aa.PAT_ENC_CSN_ID IS NOT NULL THEN 1 ELSE 0 END) AS FLOAT) * 100.0 / COUNT(DISTINCT penc.PAT_ENC_CSN_ID) AS ambient_pct
FROM PAT_ENC penc
INNER JOIN date_filtered_enc dfe
        ON dfe.PAT_ENC_CSN_ID = penc.PAT_ENC_CSN_ID
LEFT JOIN all_ambient aa
       ON aa.PAT_ENC_CSN_ID = penc.PAT_ENC_CSN_ID
LEFT JOIN ZC_DISP_ENC_TYPE zenc
       ON penc.ENC_TYPE_C = zenc.DISP_ENC_TYPE_C
GROUP BY
  penc.ENC_TYPE_C,
  zenc.NAME
ORDER BY
  total_encounters DESC;

/* As a reminder, any information Abridge shares with you related to our internal processes, 
including our Epic scripts and queries, is considered confidential information under the services agreement between your health system and Abridge. 
Accordingly, Abridge's information generally may not be shared with third parties. */
