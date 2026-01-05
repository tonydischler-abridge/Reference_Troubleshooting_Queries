date_filtered_enc AS (
  SELECT e.PAT_ENC_CSN_ID, e.CONTACT_DATE
  FROM PAT_ENC e
  CROSS JOIN DATE_PARAMS dp
  WHERE e.CONTACT_DATE >= dp.START_DATE
    AND e.CONTACT_DATE <= dp.END_DATE
    AND e.ENC_TYPE_C IN (3,76,101) -- hospital, telemedicine, and office visit
    AND (e.APPT_STATUS_C IN (2,6) OR e.APPT_STATUS_C IS NULL) -- checking for completed or arrived or null
    AND e.visit_prov_id IN (X,Y,X) -- Enter ID's of Providers you which to include in query
),
