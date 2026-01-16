/*
This query tags encounters using abridge or not through ambient session and Smartdata element usage. Then we pull count information for all encounters and show a percentage of ambient usage by encounter and vsiit type.

The final result is a daily count with a row for each combination of encounter, specialty, and visit type. This query also looks at when those encounters were closed and buckets them into groups of closed within 24, 48, 72 or more than 72 hours.
The last select and group by can be updated for granularity.
*/
WITH
-- date parameters
DATE_PARAMS AS (
  SELECT
    CAST('2024-01-01' AS date) AS START_DATE,
    CAST(GETDATE() AS date)    AS END_DATE     -- "today"
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
ambient_via_sde_all AS (
  SELECT DISTINCT
    e.PAT_ENC_CSN_ID,
    hno.ENTRY_USER_ID AS note_author_user_id,
    hno.NOTE_ID
  FROM date_filtered_enc e
  INNER JOIN SMRTDTA_ELEM_DATA sde
    ON sde.CONTACT_SERIAL_NUM = e.PAT_ENC_CSN_ID
  INNER JOIN SMRTDTA_ELEM_VALUE sev
    ON sev.HLV_ID = sde.HLV_ID
  INNER JOIN HNO_INFO hno
    ON hno.PAT_ENC_CSN_ID = e.PAT_ENC_CSN_ID
  WHERE sde.ELEMENT_ID IN ('EPIC#31000231848','EPIC#31000231852','EPIC#31000231850','EPIC#31000231851','EPIC#31000231856')-- these are the standard smartdata element values for Abridge
),
ambient_via_sde AS (
  SELECT
    PAT_ENC_CSN_ID,
    note_author_user_id
  FROM (
    SELECT
      PAT_ENC_CSN_ID,
      note_author_user_id,
      ROW_NUMBER() OVER (PARTITION BY PAT_ENC_CSN_ID ORDER BY NOTE_ID) AS rn
    FROM ambient_via_sde_all
  ) ranked
  WHERE rn = 1
),

-- which encounters had an ambient session
ambient_via_nas_all AS (
  SELECT DISTINCT
    e.PAT_ENC_CSN_ID,
    hno.ENTRY_USER_ID AS note_author_user_id,
    hno.NOTE_ID
  FROM date_filtered_enc e
  INNER JOIN HNO_INFO hno
    ON hno.PAT_ENC_CSN_ID = e.PAT_ENC_CSN_ID
  INNER JOIN NOTE_AMBIENT_SECTIONS nas
    ON nas.NOTE_ID = hno.NOTE_ID
  WHERE nas.AMBIENT_SESSION_IDENT IS NOT NULL
),
ambient_via_nas AS (
  SELECT
    PAT_ENC_CSN_ID,
    note_author_user_id
  FROM (
    SELECT
      PAT_ENC_CSN_ID,
      note_author_user_id,
      ROW_NUMBER() OVER (PARTITION BY PAT_ENC_CSN_ID ORDER BY NOTE_ID) AS rn
    FROM ambient_via_nas_all
  ) ranked
  WHERE rn = 1
),

--ambient via note attribution
ambient_via_attr_all AS (
  SELECT DISTINCT
    e.PAT_ENC_CSN_ID,
    hno.ENTRY_USER_ID AS note_author_user_id,
    hno.NOTE_ID
  FROM date_filtered_enc e
  INNER JOIN HNO_INFO hno
    ON hno.PAT_ENC_CSN_ID = e.PAT_ENC_CSN_ID
  INNER JOIN NOTE_ATTRIBUTION na
    ON na.NOTE_ID = hno.NOTE_ID
  WHERE na.NOTEATTR_SOURCE_C = 25
),
ambient_via_attr AS (
  SELECT
    PAT_ENC_CSN_ID,
    note_author_user_id
  FROM (
    SELECT
      PAT_ENC_CSN_ID,
      note_author_user_id,
      ROW_NUMBER() OVER (PARTITION BY PAT_ENC_CSN_ID ORDER BY NOTE_ID) AS rn
    FROM ambient_via_attr_all
  ) ranked
  WHERE rn = 1
),

-- ambient from dxr
ambient_via_dxr AS (
  SELECT DISTINCT
    e.PAT_ENC_CSN_ID,
    CAST(NULL AS VARCHAR(18)) AS note_author_user_id  -- DXR uses external documents, no HNO_INFO link
  FROM date_filtered_enc e
  INNER JOIN DOCS_RCVD_DETAILS d
    ON d.DOCUMENT_RQST_CSN = e.PAT_ENC_CSN_ID
  INNER JOIN DOCS_RCVD_NOTE_SECTIONS n
    ON n.DOCUMENT_ID = d.DOCUMENT_ID
  WHERE d.DOCUMENT_RQST_CSN IS NOT NULL
),

-- 3) Combine all ambient sources - each source already returns max 1 row per encounter
all_ambient AS (
  SELECT PAT_ENC_CSN_ID, note_author_user_id FROM ambient_via_nas
  UNION
  SELECT PAT_ENC_CSN_ID, note_author_user_id FROM ambient_via_sde
  UNION
  SELECT PAT_ENC_CSN_ID, note_author_user_id FROM ambient_via_attr
  UNION
  SELECT PAT_ENC_CSN_ID, note_author_user_id FROM ambient_via_dxr
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
    -- Use note author if available, otherwise fall back to visit provider
    COALESCE(note_ser.PROV_ID, penc.VISIT_PROV_ID) AS prov_id,
    COALESCE(note_ser.PROV_NAME, visit_ser.PROV_NAME) AS prov_name,
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
  -- Join to get note author provider info
  LEFT JOIN CLARITY_EMP note_emp
         ON note_emp.USER_ID = aa.note_author_user_id
  LEFT JOIN CLARITY_SER note_ser
         ON note_ser.PROV_ID = note_emp.PROV_ID
  -- Keep visit provider as fallback
  LEFT JOIN CLARITY_SER visit_ser
         ON visit_ser.PROV_ID = penc.VISIT_PROV_ID
  LEFT JOIN ZC_DISP_ENC_TYPE zenc
         ON penc.ENC_TYPE_C = zenc.DISP_ENC_TYPE_C
  LEFT JOIN CLARITY_PRC prc
         ON penc.APPT_PRC_ID = prc.PRC_ID
  LEFT JOIN CLARITY_DEP dep
         ON penc.DEPARTMENT_ID = dep.DEPARTMENT_ID
  WHERE penc.APPT_STATUS_C IN (2, 6) -- add additional filtering as needed to narrow encounter and visit types for site specific reporting
)
-- Final aggregation: counts and percent by day, encounter type. visit type information is commented out, uncomment to add back in if desired for analysis
SELECT
  encounter_date,
  encounter_type_code,
  encounter_type_name,
  --visit_type_code,
  --visit_type_name,
  prov_id,
  prov_name,
  department_name,
  department_specialty,
  SUM(CASE WHEN abridge_flag = 1 AND closure_hours <= 24 THEN 1 ELSE 0 END) AS closed_within_24h_abridge,
  SUM(CASE WHEN abridge_flag = 0 AND closure_hours <= 24 THEN 1 ELSE 0 END) AS closed_within_24h_nonabridge,
  SUM(CASE WHEN abridge_flag = 1 AND closure_hours > 24 AND closure_hours <= 48 THEN 1 ELSE 0 END) AS closed_within_48h_abridge,
  SUM(CASE WHEN abridge_flag = 0 AND closure_hours > 24 AND closure_hours <= 48 THEN 1 ELSE 0 END) AS closed_within_48h_nonabridge,
  SUM(CASE WHEN abridge_flag = 1 AND closure_hours > 48 AND closure_hours <= 72 THEN 1 ELSE 0 END) AS closed_within_72h_abridge,
  SUM(CASE WHEN abridge_flag = 0 AND closure_hours > 48 AND closure_hours <= 72 THEN 1 ELSE 0 END) AS closed_within_72h_nonabridge,
  SUM(CASE WHEN abridge_flag = 1 AND closure_hours > 72 THEN 1 ELSE 0 END) AS closed_after_72h_abridge,
  SUM(CASE WHEN abridge_flag = 0 AND closure_hours > 72 THEN 1 ELSE 0 END) AS closed_after_72h_nonabridge,
  COUNT(DISTINCT encounter_csn) AS total_encounters,
  SUM(abridge_flag) AS abridge_encounters,
  SUM(abridge_flag) * 100.0 / COUNT(DISTINCT encounter_csn) AS abridge_pct
FROM ENCOUNTER_INFO
GROUP BY
  encounter_date,
  encounter_type_code,
  encounter_type_name,
  --visit_type_code, --Remove comment if you want to also aggregate by visit type
  --visit_type_name, --Remove comment if you want to also aggregate by visit type
  prov_id,
  prov_name,
  department_name,
  department_specialty
ORDER BY
  encounter_date,
  encounter_type_code,
  --visit_type_code, --Remove comment if you want to also order by visit type
  prov_id,
  department_name;

/* As a reminder, any information Abridge shares with you related to our internal processes, 
including our Epic scripts and queries, is considered confidential information under the services agreement between your health system and Abridge. 
Accordingly, Abridge's information generally may not be shared with third parties. */
