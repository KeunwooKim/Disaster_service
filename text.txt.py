CREATE TABLE IF NOT EXISTS ForecastAnnouncement (
    announce_no uuid PRIMARY KEY,
    disaster_region text,
    alert_type text,
    announce_time timestamp,
    comment text
);