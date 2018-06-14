CREATE MATERIALIZED VIEW CURRENT_ROAD_LINK_MV
   BUILD IMMEDIATE
   REFRESH FAST ON COMMIT
   AS SELECT
    r.ID, r.ROAD_NUMBER, r.ROAD_PART_NUMBER, r.TRACK_CODE, r.DISCONTINUITY, r.START_ADDR_M, r.END_ADDR_M, r.LRM_POSITION_ID,
    r.START_DATE, r.END_DATE, r.CREATED_BY, r.VALID_FROM, r.CALIBRATION_POINTS, r.FLOATING, r.GEOMETRY, r.VALID_TO, r.ELY,
    r.ROAD_TYPE, r.TERMINATED, r.COMMON_HISTORY_ID,
    l.LANE_CODE, l.SIDE_CODE, l.START_MEASURE, l.END_MEASURE, l.MML_ID, l.LINK_ID, l.ADJUSTED_TIMESTAMP, l.MODIFIED_DATE, l.LINK_SOURCE
  FROM ROAD_ADDRESS r, LRM_POSITION l
  WHERE r.LRM_POSITION_ID = l.ID AND r.END_DATE IS NULL AND r.VALID_TO IS NULL
;
