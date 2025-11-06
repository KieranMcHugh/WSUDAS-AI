-- charset baseline
SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

USE service_models;

-- region id
SELECT id INTO @region_id
FROM service_models.contour_regions
WHERE CONVERT(name USING utf8mb4) COLLATE utf8mb4_unicode_ci = _utf8mb4'Test 1' COLLATE utf8mb4_unicode_ci
LIMIT 1;

-- insert new locations
INSERT INTO service_models.contour_locations
(name, lat, lng, survey_year, contour_region_id, created_at, created_by)
SELECT
    wt.name, wt.lat, wt.lng,
    YEAR(CURDATE()), @region_id, NOW(), 1
FROM service_weather.scout_labs_traps wt
         LEFT JOIN service_models.contour_locations ml
                   ON CONVERT(ml.name USING utf8mb4) COLLATE utf8mb4_unicode_ci
                          = CONVERT(wt.name USING utf8mb4) COLLATE utf8mb4_unicode_ci
                       AND ml.contour_region_id = @region_id
                       AND ml.survey_year = YEAR(CURDATE())
WHERE ml.id IS NULL
  AND wt.lat IS NOT NULL
  AND wt.lng IS NOT NULL;

-- time window for counts
SET @from := '2024-01-01';
SET @to   := '2025-12-31';

DROP TEMPORARY TABLE IF EXISTS tmp_counts_slice;
CREATE TEMPORARY TABLE tmp_counts_slice AS
SELECT
    ml.id AS location_id,
    DATE(wr.recorded_at) AS survey_date,
    COALESCE(wr.detection_count, 0) AS trap_count
FROM service_weather.scout_labs_records wr
         JOIN service_weather.scout_labs_traps wt
              ON CAST(wt.trap_id AS CHAR(36)) = CAST(wr.trap_id AS CHAR(36))
         JOIN service_models.contour_locations ml
              ON CONVERT(ml.name USING utf8mb4) COLLATE utf8mb4_unicode_ci
                     = CONVERT(wt.name USING utf8mb4) COLLATE utf8mb4_unicode_ci
                  AND ml.contour_region_id = @region_id
                  AND ml.survey_year = YEAR(wr.recorded_at)
WHERE wr.recorded_at >= @from AND wr.recorded_at < @to
  AND NOT EXISTS (
    SELECT 1
    FROM service_models.contour_trap_counts c
    WHERE c.location_id = ml.id
      AND c.survey_date = DATE(wr.recorded_at)
);
