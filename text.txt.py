DELETE FROM rtd_db WHERE rtd_code = 33 AND rtd_time IN ('2025-04-02 00:00:00', '2025-04-05 12:00:00') AND id IN (uuid1, uuid2);
SELECT rtd_code, rtd_time, id FROM rtd_db WHERE rtd_time > '2025-04-01 00:00:00' ALLOW FILTERING;
