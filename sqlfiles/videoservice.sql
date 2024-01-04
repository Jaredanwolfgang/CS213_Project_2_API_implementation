--Function 1: Create Video
--Generate BV function:
CREATE OR REPLACE FUNCTION generate_bv ()
RETURNS VARCHAR AS $$
DECLARE
    generated_bv    VARCHAR;
BEGIN
    SELECT 'BV' || substr(md5(random()::text), 1, 10) INTO generated_bv;
    WHILE (EXISTS(SELECT 1 FROM video_info WHERE video_info.bv = generated_bv)) LOOP
        SELECT 'BV' || substr(md5(random()::text), 1, 10) INTO generated_bv;
    END LOOP;
    RETURN generated_bv;
END;
$$ LANGUAGE plpgsql;
select generate_bv();

CREATE OR REPLACE FUNCTION create_video (
    mid_input           BIGINT,
    password_input      VARCHAR,
    qq_input            VARCHAR,
    wechat_input        VARCHAR,

    title_input         VARCHAR,
    description_input   VARCHAR,
    duration_input      NUMERIC,
    publictime_input    TIMESTAMP
) RETURNS VARCHAR AS $$
DECLARE
    bv VARCHAR;
    owner_input BIGINT;
    is_valid BOOLEAN;
BEGIN
    SELECT is_auth_valid(mid_input, password_input, qq_input, wechat_input) INTO owner_input;
    IF owner_input = 0 THEN
        RETURN NULL;
    END IF;
    SELECT is_video_valid(mid_input, title_input) INTO is_valid;
    IF is_valid IS FALSE THEN
        RETURN NULL;
    END IF;
    SELECT generate_bv() INTO bv;
    INSERT INTO video_info (BV, title, ownerMID, commitTime, publicTime, duration, description)
    VALUES (bv, title_input, owner_input, now(), publictime_input, duration_input, description_input);
    RETURN bv;
END;
$$ LANGUAGE plpgsql;

--Function 2: to delete a certain video.
CREATE OR REPLACE FUNCTION delete_video (
    mid_input           BIGINT,
    password_input      VARCHAR,
    qq_input            VARCHAR,
    wechat_input        VARCHAR,
    bv_input            VARCHAR
) RETURNS BOOLEAN AS $$
DECLARE
    deleter_input       BIGINT;
    deleter_identity    SMALLINT;
    owner_mid           BIGINT;
    owner_identity      SMALLINT;
BEGIN
    SELECT is_auth_valid(mid_input, password_input, qq_input, wechat_input) INTO deleter_input;
    IF deleter_input = 0 THEN
        RETURN FALSE;
    END IF;


    SELECT ownermid INTO owner_mid FROM video_info WHERE video_info.bv = bv_input;
    IF(owner_mid IS NULL) THEN
        RETURN FALSE;
    END IF;

    SELECT identity INTO deleter_identity FROM user_info WHERE user_info.mid = deleter_input;
    --SELECT identity INTO owner_identity FROM user_info WHERE user_info.mid = owner_mid;

    IF(deleter_identity = 0 AND deleter_input != owner_mid) THEN
        RETURN FALSE;
    ELSE
        DELETE FROM video_info WHERE video_info.bv = bv_input;
        RETURN TRUE;
    END IF;
END;
$$ LANGUAGE plpgsql;

--Function 3: to update a certain video.
CREATE OR REPLACE FUNCTION update_video (
    mid_input           BIGINT,
    password_input      VARCHAR,
    qq_input            VARCHAR,
    wechat_input        VARCHAR,
    bv_input            VARCHAR,
    title_input         VARCHAR,
    description_input   VARCHAR,
    duration_input      NUMERIC,
    publictime_input    TIMESTAMP
) RETURNS BOOLEAN AS $$
DECLARE
    auth_output BIGINT;
    is_valid BOOLEAN;
    video_owner BIGINT;
    video_title VARCHAR;
    video_description   VARCHAR;
    video_duration  NUMERIC;
    video_reviewer  BIGINT;
    video_publictime TIMESTAMP;
BEGIN
    SELECT is_auth_valid(mid_input, password_input, qq_input, wechat_input) INTO auth_output;
    IF auth_output = 0 THEN
        RETURN FALSE;
    END IF;
    SELECT is_video_valid(mid_input, title_input) INTO is_valid;
    IF is_valid IS FALSE THEN
        RETURN FALSE;
    END IF;
    
    WITH video_tables AS (
        SELECT video_info.ownermid,  
        video_info.title, 
        video_info.description, 
        video_info.duration, 
        video_info.reviewermid, 
        video_info.publictime 
        FROM video_info WHERE bv = bv_input
    )SELECT ownermid, title, description, duration, reviewermid, publictime INTO
        video_owner, video_title, video_description, video_duration, video_reviewer, video_publictime
    FROM video_tables;

    IF(video_owner <> auth_output
    OR video_reviewer IS NULL
    OR video_duration <> duration_input
    OR (video_description = description_input 
    AND video_title = title_input
    AND video_publictime = publictime_input)) THEN
        RETURN FALSE;
    ELSE
        UPDATE video_info SET title = title_input, description = description_input, publictime = publictime_input,
        reviewermid = NULL, reviewtime = NULL WHERE bv = bv_input;
        RETURN TRUE;
    END IF;
END;
$$ LANGUAGE plpgsql;

--Function 4: to search a video.
CREATE OR REPLACE FUNCTION search_video(
    mid_input           BIGINT,
    password_input      VARCHAR,
    qq_input            VARCHAR,
    wechat_input        VARCHAR,
    keywords            VARCHAR [],
    pageSize            INTEGER,
    pageNum             INTEGER
) RETURNS VARCHAR [] AS $$
DECLARE
    auth_input  BIGINT;
    username    VARCHAR;
    bv_list     VARCHAR [];
BEGIN
    SELECT is_auth_valid(mid_input, password_input, qq_input, wechat_input) INTO auth_input;
    IF auth_input = 0 THEN
        RETURN '{}';
    END IF;
    IF is_super_user(auth_input) THEN
        --If the user is super user, then he can search any video.
        WITH search_result_superuser AS
        (SELECT video_info.bv AS bv, 
        relevance_count(video_info.title, keywords) 
        + relevance_count(COALESCE(video_info.description, ''), keywords)
        + relevance_count(user_info.name, keywords) AS relevance,
        count_watch.watch_count AS watch_count
        FROM video_info
        JOIN count_watch ON video_info.bv = count_watch.bv
        JOIN user_info ON video_info.ownerMID = user_info.mid
        ORDER BY relevance DESC, watch_count DESC
        LIMIT pageSize
        OFFSET (pageNum - 1) * pageSize)
        SELECT ARRAY(SELECT bv FROM search_result_superuser WHERE search_result_superuser.relevance <> 0) INTO bv_list;
    ELSE
        --If the user is not a super user, he can only search published video and the video of his own.
        WITH search_result_user AS 
        (SELECT video_info.bv AS bv, 
        relevance_count(video_info.title, keywords) 
        + relevance_count(COALESCE(video_info.description, ''), keywords)
        + relevance_count(user_info.name, keywords) AS relevance,
        count_watch.watch_count AS watch_count
        FROM video_info
        JOIN count_watch ON video_info.bv = count_watch.bv
        JOIN user_info ON video_info.ownerMID = user_info.mid
        where (video_info.reviewtime IS NOT NULL
        AND video_info.publictime < NOW()) 
        OR (video_info.ownerMID = auth_input)
        ORDER BY relevance DESC, watch_count DESC
        LIMIT pageSize
        OFFSET (pageNum - 1) * pageSize)
        SELECT ARRAY(SELECT bv FROM search_result_user WHERE search_result_user.relevance <> 0) INTO bv_list;
    END IF;
    RETURN bv_list;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION relevance_count(
    input_              VARCHAR,
    keywords            VARCHAR []
) RETURNS INTEGER AS $$
DECLARE
    relevance INTEGER;
BEGIN   
    relevance := 0;
    FOR i IN 1..array_length(keywords, 1) LOOP
        relevance := relevance + regexp_count(input_, keywords[i], 1, 'i');
    END LOOP;
    RETURN relevance;
END;
$$ LANGUAGE plpgsql;

--Function 5: get average view rate.
CREATE OR REPLACE FUNCTION get_average_view_rate (
    bv_input            VARCHAR
) RETURNS NUMERIC AS $$
DECLARE
    view_rate_ NUMERIC;
    bv_ VARCHAR;
BEGIN
    SELECT is_bv_valid_for_users(bv_input) INTO bv_;
    IF bv_ IS NULL THEN
        RETURN -1;
    END IF;
    SELECT watch_rate.watch_rate INTO view_rate_ FROM watch_rate WHERE watch_rate.bv = bv_input;
    RETURN view_rate_;
END;
$$ LANGUAGE plpgsql;
SELECT get_average_view_rate('BV1tf4y1D75w');
SELECT is_bv_valid_for_users('BV1tf4y1D75w');

--Function 6: Find the hotspot of the video
CREATE OR REPLACE FUNCTION find_hotspot (
    bv_input            VARCHAR
) RETURNS INTEGER [] AS $$
DECLARE
    bv_ VARCHAR;
    danmu_count INTEGER;
    chunk_hotspot_list INTEGER [];
BEGIN
    --Cannot find a video corresponding to the bv
    SELECT is_bv_valid_for_users(bv_input) INTO bv_;
    IF bv_ IS NULL THEN
        RETURN '{}';
    END IF;
    --No one has sent danmu on this video
    SELECT count(*) INTO danmu_count FROM danmu WHERE danmu.bv = bv_input;
    IF danmu_count = 0 THEN
        RETURN '{}';
    END IF;

    SELECT ARRAY
    (SELECT index_ FROM
        (SELECT chunk_count.index_, chunk_count.count_, max(chunk_count.count_) over () AS max_ FROM 
        (SELECT floor(danmu.displaytime / 10) AS index_, count(*) AS count_
        FROM danmu 
        WHERE danmu.bv = bv_
        GROUP BY index_) AS chunk_count)
    WHERE count_ = max_
    ORDER BY index_) INTO chunk_hotspot_list;
    RETURN chunk_hotspot_list;
END;
$$ LANGUAGE plpgsql;

--Function 7: Review Video
CREATE OR REPLACE FUNCTION review_video (
    reviewer_input      BIGINT,
    password_input      VARCHAR,
    qq_input            VARCHAR,
    wechat_input        VARCHAR,
    bv_input            VARCHAR
) RETURNS BOOLEAN AS $$
DECLARE
    reviewer_auth_input BIGINT;
    valid_bv VARCHAR;
BEGIN
    SELECT is_auth_valid(reviewer_input, password_input, qq_input, wechat_input) INTO reviewer_auth_input;
    IF reviewer_auth_input = 0 THEN
        RETURN FALSE;
    END IF;
    IF is_super_user(reviewer_auth_input) IS FALSE THEN
        RETURN FALSE;
    ELSE 
        SELECT is_bv_valid_for_superusers(bv_input) INTO valid_bv;
        IF (EXISTS(SELECT 1 FROM video_info WHERE video_info.bv = valid_bv
            AND video_info.reviewerMID IS NULL 
            AND video_info.ownermid <> reviewer_auth_input)) THEN
            UPDATE video_info SET reviewTime = now(), reviewerMID = reviewer_auth_input WHERE video_info.bv = bv_input;
            RETURN TRUE;
        ELSE 
            RETURN FALSE;
        END IF;
    END IF;
END;
$$ LANGUAGE plpgsql;

--Function 8: Coin Video
CREATE OR REPLACE FUNCTION coin_video (
    mid_input           BIGINT,
    password_input      VARCHAR,
    qq_input            VARCHAR,
    wechat_input        VARCHAR,
    bv_input            VARCHAR
) RETURNS BOOLEAN AS $$
DECLARE
    user_output BIGINT;
    valid_bv VARCHAR;
BEGIN
    SELECT is_auth_valid(mid_input, password_input, qq_input, wechat_input) INTO user_output;
    IF user_output = 0 THEN
        RETURN FALSE;
    END IF;
    IF is_super_user(user_output) IS TRUE THEN
        SELECT is_bv_valid_for_superusers(bv_input) INTO valid_bv;
    ELSE 
        SELECT is_bv_valid_for_users(bv_input) INTO valid_bv;
    END IF;
    IF valid_bv IS NULL THEN
        RETURN FALSE;
    END IF;
    IF (EXISTS(SELECT 1 FROM coin WHERE coin.bv = valid_bv AND coin.mid = user_output) 
    OR EXISTS(SELECT 1 FROM user_info WHERE user_info.mid = user_output AND user_info.coin = 0)
    OR EXISTS(SELECT 1 FROM video_info WHERE video_info.bv = valid_bv AND video_info.ownermid = user_output)) THEN
        RETURN FALSE;
    ELSE 
        INSERT INTO coin (bv, mid) VALUES (bv_input, user_output);
        RETURN TRUE;
    END IF;
END;
$$ LANGUAGE plpgsql;

--Function 9: Like Video
CREATE OR REPLACE FUNCTION like_video (
    mid_input           BIGINT,
    password_input      VARCHAR,
    qq_input            VARCHAR,
    wechat_input        VARCHAR,
    bv_input            VARCHAR
) RETURNS BOOLEAN AS $$
DECLARE
    user_input BIGINT;
    valid_bv VARCHAR;
BEGIN
    --If the auth is valid.
    SELECT is_auth_valid(mid_input, password_input, qq_input, wechat_input) INTO user_input;
    IF user_input = 0 THEN
        RETURN FALSE;
    END IF;
    --If the video is not valid for the user.
    IF is_super_user(user_input) THEN
        SELECT is_bv_valid_for_superusers(bv_input) INTO valid_bv;
    ELSE 
        SELECT is_bv_valid_for_users(bv_input) INTO valid_bv;
    END IF;
    IF valid_bv IS NULL THEN
        RETURN FALSE;
    END IF;
    --If the video belongs to the user, you cannot like it. 
    IF (EXISTS(SELECT 1 FROM video_info WHERE video_info.bv = valid_bv AND video_info.ownermid = user_input)) THEN
        RETURN FALSE;
    --Already liked the video
    ELSIF (EXISTS(SELECT 1 FROM like_video WHERE like_video.bv = valid_bv AND like_video.mid = user_input)) THEN
        DELETE FROM like_video WHERE like_video.bv = valid_bv AND like_video.mid = user_input;
        RETURN FALSE;
    ELSE
        INSERT INTO like_video (bv, mid) VALUES (valid_bv, user_input);
        RETURN TRUE;
    END IF;
END;
$$ LANGUAGE plpgsql;

--Function 10: Collect Video
CREATE OR REPLACE FUNCTION collect_video (
    mid_input           BIGINT,
    password_input      VARCHAR,
    qq_input            VARCHAR,
    wechat_input        VARCHAR,
    bv_input            VARCHAR
) RETURNS BOOLEAN AS $$
DECLARE
    user_input BIGINT;
    valid_bv VARCHAR;
BEGIN
    SELECT is_auth_valid(mid_input, password_input, qq_input, wechat_input) INTO user_input;
    IF user_input = 0 THEN
        RETURN FALSE;
    END IF;

    IF is_super_user(user_input) IS TRUE THEN
        SELECT is_bv_valid_for_superusers(bv_input) INTO valid_bv;
    ELSE 
        SELECT is_bv_valid_for_users(bv_input) INTO valid_bv;
    END IF;
    IF valid_bv IS NULL THEN
        RETURN FALSE;
    END IF;

    IF (EXISTS(SELECT 1 FROM favorite WHERE favorite.bv = bv_input AND favorite.mid = user_input)
    OR EXISTS(SELECT 1 FROM video_info WHERE video_info.bv = bv_input AND video_info.ownermid = user_input)) THEN
        RETURN FALSE;
    ELSE 
        INSERT INTO favorite (bv, mid) VALUES (bv_input, user_input);
        RETURN TRUE;
    END IF;
END;
$$ LANGUAGE plpgsql;