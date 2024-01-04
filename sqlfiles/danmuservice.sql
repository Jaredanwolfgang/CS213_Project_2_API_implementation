
CREATE OR REPLACE FUNCTION send_danmu(
    mid_input            BIGINT,
    password_input      VARCHAR,
    qq_input            VARCHAR,
    wechat_input        VARCHAR,
    input_bv            VARCHAR,
    input_time          NUMERIC,
    input_content       VARCHAR
) RETURNS BIGINT AS $$
DECLARE
    mid_output      BIGINT;
    bv_watched      VARCHAR [];
    bv_             VARCHAR;
    time_valid      INTEGER;
    danmu_id_       BIGINT;
BEGIN
    SELECT is_auth_valid(mid_input, password_input, qq_input, wechat_input) INTO mid_output;
    IF  (mid_output = 0) THEN
        RETURN -1;
    END IF;

    SELECT ARRAY(SELECT bv FROM watch WHERE mid = mid_output) INTO bv_watched;
    SELECT is_bv_valid_for_users (input_bv) INTO bv_;
    IF(bv_ IS NULL) OR (bv_ <> ALL(bv_watched))THEN
        RETURN -1;
    END IF;

    SELECT duration FROM video_info WHERE bv = bv_ INTO time_valid;
    IF(input_time > time_valid) THEN
        RETURN -1;
    END IF;

    INSERT INTO danmu (bv, displaytime, content, mid, posttime) VALUES (input_bv, input_time, input_content, mid_output, NOW()) 
    RETURNING danmu_id INTO danmu_id_;
    RETURN danmu_id_;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION display_Danmu_with_Repetition(
    input_bv    varchar,
    timeStart   numeric,
    timeEnd     numeric
) RETURNS BIGINT[] AS $$
DECLARE
    bv_             VARCHAR;
    time_valid      INTEGER;
    danmu_output    BIGINT[];
BEGIN
    SELECT is_bv_valid_for_users (input_bv) INTO bv_;
    IF(bv_ IS NULL) THEN
        RETURN '{}';
    END IF;
    SELECT time_valid(input_bv, timeStart, timeEnd) INTO time_valid;
    IF(time_valid = 0) THEN
        RETURN '{}';
    END IF;
    
    SELECT ARRAY(SELECT d.danmu_id
    from danmu as d
    where d.bv = input_bv and d.displaytime between timeStart and timeEnd)
    INTO danmu_output;
    RETURN danmu_output;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION display_Danmu_distinct(
    input_bv    varchar,
    timeStart   numeric,
    timeEnd     numeric
) RETURNS BIGINT[] AS $$
DECLARE
    bv_             VARCHAR;
    time_valid      INTEGER;
    danmu_output    BIGINT[];
BEGIN
    SELECT is_bv_valid_for_users (input_bv) INTO bv_;
    IF(bv_ IS NULL) THEN
        RETURN '{}';
    END IF;
    SELECT time_valid(input_bv, timeStart, timeEnd) INTO time_valid;
    IF(time_valid = 0) THEN
        RETURN '{}';
    END IF;
    
    SELECT ARRAY(SELECT danmu_id FROM
    (SELECT d.content, d.danmu_id, d.displaytime, min(displaytime) over (partition by d.content) AS earliest_time
    from danmu as d
    where d.bv = input_bv and d.displaytime between timeStart and timeEnd)
    WHERE displaytime = earliest_time
    ORDER BY displaytime)
    INTO danmu_output;
    RETURN danmu_output;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION is_danmu_valid (
    search_id   BIGINT
)RETURNS BIGINT AS $$
DECLARE
    danmu_id_   BIGINT;
BEGIN
    SELECT d.danmu_id from danmu as d where d.danmu_id = search_id INTO danmu_id_;
    IF(danmu_id_ IS NULL) THEN
        RETURN 0;
    ELSE 
        RETURN danmu_id_;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION like_danmu(
    mid_input            BIGINT,
    password_input      VARCHAR,
    qq_input            VARCHAR,
    wechat_input        VARCHAR,
    danmu_id_input      BIGINT
) RETURNS BIGINT AS $$
DECLARE
    mid_output      BIGINT;
    danmu_output    BIGINT;
    bv_watched      VARCHAR [];
    bv_             VARCHAR;
    input_bv        VARCHAR;
BEGIN
    SELECT is_auth_valid(mid_input, password_input, qq_input, wechat_input) INTO mid_output;
    IF mid_output = 0 THEN
        RETURN -1;
    END IF;

    SELECT is_danmu_valid(danmu_id_input) INTO danmu_output;
    IF danmu_output = 0  THEN
        RETURN -1;
    END IF;

    SELECT bv FROM danmu WHERE danmu_id = danmu_output INTO input_bv;

    SELECT ARRAY(SELECT bv FROM watch WHERE mid = mid_output) INTO bv_watched;
    SELECT is_bv_valid_for_users (input_bv) INTO bv_;
    IF(bv_ IS NULL) OR (bv_ <> ALL(bv_watched))THEN
        RETURN -1;
    END IF;

    INSERT INTO like_danmu (danmu_id, mid) VALUES (danmu_output, mid_output); 
    RETURN 1;
END;
$$ LANGUAGE plpgsql;