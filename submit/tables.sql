--Table Creation
--Table User_info
CREATE TABLE User_info (
    MID             BIGINT              NOT NULL,
    name            VARCHAR             NOT NULL,
    gender          VARCHAR             ,
    birthday        VARCHAR             ,
    coin            INTEGER             NOT NULL DEFAULT 0,
    level           SMALLINT            NOT NULL DEFAULT 1,
    sign            VARCHAR             ,
    identity        SMALLINT            NOT NULL,
    password        VARCHAR             NOT NULL,
    qq              VARCHAR             ,
    wechat          VARCHAR             ,
    visible         BOOLEAN             NOT NULL DEFAULT TRUE,
    CONSTRAINT pk_user_info_xiaoyc
        PRIMARY KEY(MID)
);
        
--Table Follow
 CREATE TABLE Follow (
    follower        BIGINT,
    followee        BIGINT,
    visible         BOOLEAN             NOT NULL DEFAULT TRUE,
    CONSTRAINT  pk_follow_xiaoyc
        PRIMARY KEY (follower, followee)
);


--Table Video_info
CREATE TABLE Video_info (
    BV              VARCHAR(20),
    title           VARCHAR(255)    NOT NULL,
    ownerMID        BIGINT,
    commitTime      TIMESTAMP,
    publicTime      TIMESTAMP,
    reviewTime      TIMESTAMP,
    reviewerMID     BIGINT,
    duration        NUMERIC,
    description     TEXT,
    visible         BOOLEAN         NOT NULL DEFAULT TRUE,
    CONSTRAINT  pk_video_xiaoyc
        PRIMARY KEY (BV)
);

--Table Danmu
CREATE TABLE Danmu (
    Danmu_ID        BIGINT GENERATED ALWAYS AS IDENTITY,        
    BV              VARCHAR(255),
    MID             BIGINT,
    content         TEXT,
    displayTime     NUMERIC,
    postTime        TIMESTAMP,
    visible         BOOLEAN         NOT NULL DEFAULT TRUE,
    CONSTRAINT  pk_danmu_xiaoyc
        PRIMARY KEY (Danmu_ID)
);

--Table watch
CREATE TABLE Watch (
    BV              VARCHAR(255),
    MID             BIGINT,
    watchDuration   NUMERIC,
    visible         BOOLEAN         NOT NULL DEFAULT TRUE
);

--Table Like_video
CREATE TABLE Like_video (
    BV              VARCHAR(255),
    MID             BIGINT,
    visible         BOOLEAN         NOT NULL DEFAULT TRUE
);

--Table Coin
CREATE TABLE Coin (
    BV              VARCHAR(255),
    MID             BIGINT,
    visible         BOOLEAN         NOT NULL DEFAULT TRUE
);

--Table Favorite
CREATE TABLE Favorite (
    BV              VARCHAR(255),
    MID             BIGINT,
    visible         BOOLEAN         NOT NULL DEFAULT TRUE
);

--Table Like Danmu
CREATE TABLE Like_danmu (
    Danmu_id        BIGINT,
    MID             BIGINT,
    visible         BOOLEAN         NOT NULL DEFAULT TRUE
);

--Drop All the foreign key constraints
CREATE OR REPLACE FUNCTION drop_constraints_foreign_keys()
RETURNS INTEGER AS $$
BEGIN
    ALTER TABLE follow DROP CONSTRAINT fk_follower_xiaoyc;
    ALTER TABLE follow DROP CONSTRAINT fk_followee_xiaoyc;
    ALTER TABLE video_info DROP CONSTRAINT fk_owner_xiaoyc;
    ALTER TABLE video_info DROP CONSTRAINT fk_reviewer_xiaoyc;
    ALTER TABLE danmu DROP CONSTRAINT fk_danmu_bv_xiaoyc;
    ALTER TABLE danmu DROP CONSTRAINT fk_danmu_mid_xiaoyc;
    ALTER TABLE watch DROP CONSTRAINT fk_bv_watch_xiaoyc;
    ALTER TABLE watch DROP CONSTRAINT fk_mid_watch_xiaoyc;
    ALTER TABLE Like_video DROP CONSTRAINT fk_bv_like_xiaoyc;
    ALTER TABLE Like_video DROP CONSTRAINT fk_mid_like_xiaoyc;
    ALTER TABLE coin DROP CONSTRAINT fk_bv_coin_xiaoyc;
    ALTER TABLE coin DROP CONSTRAINT fk_mid_coin_xiaoyc;
    ALTER TABLE favorite DROP CONSTRAINT fk_bv_favorite_xiaoyc;
    ALTER TABLE favorite DROP CONSTRAINT fk_mid_favorite_xiaoyc;
    ALTER TABLE like_danmu DROP CONSTRAINT fk_danmu_id_like_xiaoyc;
    ALTER TABLE like_danmu DROP CONSTRAINT fk_mid_like_for_danmu_xiaoyc;
    RETURN 1;
END;
$$ LANGUAGE plpgsql;

--Add all the foreign key constraints
CREATE OR REPLACE FUNCTION add_constraints_foreign_keys()
RETURNS INTEGER AS $$
BEGIN
    ALTER TABLE follow ADD CONSTRAINT fk_follower_xiaoyc
            FOREIGN KEY (follower)
            REFERENCES User_info (MID)
            ON DELETE CASCADE;
    ALTER TABLE Follow ADD CONSTRAINT fk_followee_xiaoyc
            FOREIGN KEY (followee)
            REFERENCES User_info (MID)
            ON DELETE CASCADE;
    ALTER TABLE video_info ADD CONSTRAINT  fk_owner_xiaoyc
            FOREIGN KEY (ownerMID)
            REFERENCES User_info (MID)
            ON DELETE CASCADE;
    ALTER TABLE video_info ADD CONSTRAINT fk_reviewer_xiaoyc
            FOREIGN KEY (reviewerMID)
            REFERENCES User_info (MID)
            ON DELETE CASCADE;
    ALTER TABLE danmu ADD CONSTRAINT  fk_danmu_bv_xiaoyc
            FOREIGN KEY (BV)
            REFERENCES Video_info (BV)
            ON DELETE CASCADE;
    ALTER TABLE danmu ADD CONSTRAINT fk_danmu_mid_xiaoyc
            FOREIGN KEY (MID)
            REFERENCES User_info (MID)
            ON DELETE CASCADE;
    ALTER TABLE watch ADD CONSTRAINT fk_bv_watch_xiaoyc
            FOREIGN KEY (BV)
            REFERENCES Video_info (BV)
            ON DELETE CASCADE;
    ALTER TABLE watch ADD CONSTRAINT fk_mid_watch_xiaoyc
            FOREIGN KEY (MID)
            REFERENCES User_info (MID)
            ON DELETE CASCADE;
    ALTER TABLE Like_video ADD CONSTRAINT fk_bv_like_xiaoyc
            FOREIGN KEY (BV)
            REFERENCES Video_info (BV)
            ON DELETE CASCADE;
    ALTER TABLE Like_video ADD CONSTRAINT fk_mid_like_xiaoyc
            FOREIGN KEY (MID)
            REFERENCES User_info (MID)
            ON DELETE CASCADE;
    ALTER TABLE coin ADD CONSTRAINT fk_bv_coin_xiaoyc
            FOREIGN KEY (BV)
            REFERENCES Video_info (BV)
            ON DELETE CASCADE;
    ALTER TABLE coin ADD CONSTRAINT fk_mid_coin_xiaoyc
            FOREIGN KEY (MID)
            REFERENCES User_info (MID)
            ON DELETE CASCADE;
    ALTER TABLE favorite ADD CONSTRAINT fk_bv_favorite_xiaoyc
            FOREIGN KEY (BV)
            REFERENCES Video_info (BV)
            ON DELETE CASCADE;
    ALTER TABLE favorite ADD CONSTRAINT fk_mid_favorite_xiaoyc
            FOREIGN KEY (MID)
            REFERENCES User_info (MID)
            ON DELETE CASCADE;
    ALTER TABLE like_danmu ADD CONSTRAINT fk_danmu_id_like_xiaoyc
            FOREIGN KEY (Danmu_id)
            REFERENCES Danmu (Danmu_id)
            ON DELETE CASCADE;
    ALTER TABLE like_danmu ADD CONSTRAINT  fk_mid_like_for_danmu_xiaoyc
            FOREIGN KEY (MID)
            REFERENCES User_info (MID)
            ON DELETE CASCADE;
    RETURN 1;
    END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION drop_all_indexes()
RETURNS INTEGER AS $$
BEGIN
    DROP INDEX user_index;
    DROP INDEX user_index_qq;
    DROP INDEX user_index_wechat;
    DROP INDEX follow_index;
    DROP INDEX follow_index_followee;
    DROP INDEX video_index;
    DROP INDEX video_index_owner;
    DROP INDEX danmu_index;
    DROP INDEX danmu_index_bv;
    DROP INDEX danmu_index_MID;
    DROP INDEX watch_index;
    DROP INDEX watch_index_mid;
    DROP INDEX like_video_index;
    DROP INDEX like_video_index_mid;
    DROP INDEX coin_index;
    DROP INDEX coin_index_mid;
    DROP INDEX favorite_index;
    DROP INDEX favorite_index_mid;
    DROP INDEX like_danmu_index;
    DROP INDEX like_danmu_index_mid;
    RETURN 1;
END;
$$ LANGUAGE plpgsql;

--Add all INDEXES
CREATE OR REPLACE FUNCTION add_all_indexes()
RETURNS INTEGER AS $$
BEGIN
    CREATE index user_index ON user_info (MID);
    CREATE index user_index_qq ON user_info (QQ);
    CREATE index user_index_wechat ON user_info (Wechat);
    CREATE index follow_index ON follow (follower, followee);
    CREATE index follow_index_followee ON follow (followee);
    CREATE index video_index ON video_info (BV);
    CREATE index video_index_owner ON video_info (ownerMID);
    CREATE index danmu_index ON danmu (Danmu_ID, BV, MID);
    CREATE index danmu_index_bv ON danmu (BV, MID);
    CREATE index danmu_index_MID ON danmu (MID);
    CREATE index watch_index ON watch (BV, MID);
    CREATE index watch_index_mid ON watch (MID);
    CREATE index like_video_index ON like_video (BV, MID);
    CREATE index like_video_index_mid ON like_video (MID);
    CREATE index coin_index ON coin (BV, MID);
    CREATE index coin_index_mid ON coin (MID);
    CREATE index favorite_index ON favorite (BV, MID);
    CREATE index favorite_index_mid ON favorite (MID);
    CREATE index like_danmu_index ON like_danmu (Danmu_id, MID);
    CREATE index like_danmu_index_mid ON like_danmu (MID);
    RETURN 1;
END;
$$ LANGUAGE plpgsql;

--View Creation
CREATE OR REPLACE VIEW public_video AS
    SELECT video_info.bv AS bv
    FROM video_info
    where reviewtime IS NOT NULL
    AND publictime < NOW()
    AND reviewtime < NOW();

CREATE OR REPLACE VIEW count_watch AS
    SELECT watch.bv AS bv, count(watch.bv) AS watch_count
    FROM watch
    GROUP BY watch.bv;

CREATE OR REPLACE VIEW watch_rate AS
    SELECT video_info.bv AS bv, avg(watch.watchduration/video_info.duration) AS watch_rate
    FROM video_info, watch
    WHERE video_info.bv = watch.bv
    GROUP BY video_info.bv;

--General Functions: 
--Function 1: To authenticate whether a user is valid and return the mid/
--There are three ways to authenticate:
    --User MID and Password together.
    --QQ authentication
    --Wechat authentication
--So the invalid situation will be:
    --QQ and Wechat shown together but not correspond with each OTHERS
    --Both QQ and Wechat are not provided and mid <= 0(invalid mid)
CREATE OR REPLACE FUNCTION is_auth_valid(
    mid_input BIGINT,
    password_input VARCHAR,
    qq_input VARCHAR,
    wechat_input VARCHAR
) RETURNS BIGINT AS $$
DECLARE
    user_count INTEGER;
    user_id BIGINT;
BEGIN
    -- Validate by qq and wechat
    IF qq_input IS NOT NULL AND wechat_input IS NOT NULL THEN
        SELECT COUNT(*), user_info.mid INTO user_count, user_id FROM user_info
        WHERE user_info.qq = qq_input AND user_info.wechat = wechat_input
        GROUP BY user_info.mid;
        IF user_count = 1 THEN
            RETURN user_id;
        ELSE
            RETURN 0;
        END IF;
    END IF;

    IF qq_input IS NULL AND wechat_input IS NULL AND (mid_input <= 0) THEN
        RETURN 0;
    END IF;

    -- Validate by mid and password
    IF (mid_input > 0) AND password_input IS NOT NULL THEN
        SELECT COUNT(*), user_info.mid INTO user_count, user_id FROM user_info
        WHERE user_info.mid = mid_input AND user_info.password = password_input
        GROUP BY user_info.mid;
        IF user_count = 1 THEN
            RETURN user_id;
        END IF;
    END IF;

    -- Validate by qq
    IF qq_input IS NOT NULL THEN
        SELECT user_info.mid INTO user_id FROM user_info
        WHERE user_info.qq = qq_input;
        IF user_id IS NOT NULL THEN
            RETURN user_id;
        END IF;
    END IF;

    -- Validate by wechat
    IF wechat_input IS NOT NULL THEN
        SELECT user_info.mid INTO user_id FROM user_info
        WHERE user_info.wechat = wechat_input;
        IF user_id IS NOT NULL THEN
            RETURN user_id; 
        END IF;
    END IF;

    RETURN 0;
END;
$$ LANGUAGE plpgsql;

--Function 2: To authenticate whether a bv is available in public and return the bv.
    
CREATE OR REPLACE FUNCTION is_bv_valid_for_users (
    search_bv   VARCHAR
)RETURNS VARCHAR AS $$
DECLARE
    bv_output VARCHAR;
BEGIN
    SELECT public_video.bv into bv_output from public_video where public_video.bv = search_bv;
    IF(bv_output IS NULL) THEN
        RETURN NULL;
    ELSE 
        RETURN bv_output;
    END IF;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION is_bv_valid_for_superusers (
    search_bv   VARCHAR
)RETURNS VARCHAR AS $$
DECLARE
    bv_output VARCHAR;
BEGIN
    SELECT video_info.bv into bv_output from video_info where video_info.bv = search_bv;
    IF(bv_output IS NULL) THEN
        RETURN NULL;
    ELSE 
        RETURN bv_output;
    END IF;
END;
$$ LANGUAGE plpgsql;

--Function 3: To determine whether a insertion time for danmu is valid for a video.
CREATE OR REPLACE FUNCTION time_valid(
    input_bv    varchar,
    timeStart   numeric,
    timeEnd     numeric
)RETURNS INTEGER AS $$
DECLARE
    duration_time    numeric;
BEGIN
    IF(timeEnd <= timeStart) THEN
        RETURN 0;
    ELSIF(timeStart < 0) THEN
        RETURN 0;
    END IF;
    SELECT vi.duration into duration_time from video_info as vi 
    where vi.bv = input_bv;
    IF(timeEnd > duration_time) THEN
        RETURN 0;
    END IF;
    RETURN 1;
END;
$$ LANGUAGE plpgsql;

--Function 4: Is SuperUser
CREATE OR REPLACE FUNCTION is_super_user(
    mid_input BIGINT
) RETURNS BOOLEAN AS $$
DECLARE
    identity_ SMALLINT;
BEGIN
    SELECT user_info.identity INTO identity_ FROM user_info
    WHERE user_info.mid = mid_input;
    IF identity_ = 1 THEN
        RETURN TRUE;
    ELSE
        RETURN FALSE;
    END IF;
END;
$$ LANGUAGE plpgsql;

--Function 5: Is video VALID
--<li>{@code req} is invalid
--  <ul>
--    <li>{@code title} is null or empty</li>
--    <li>there is another video with same {@code title} and same user</li>
--    <li>{@code duration} is less than 10 (so that no chunk can be divided)</li>
--    <li>{@code publicTime} is earlier than {@link LocalDateTime#now()}</li>
--  </ul>
--</li>
CREATE OR REPLACE FUNCTION is_video_valid(
    mid_input           BIGINT,
    title_input         VARCHAR
) RETURNS BOOLEAN AS $$
BEGIN
    IF EXISTS(SELECT 1 FROM video_info WHERE video_info.title = title_input AND video_info.ownerMID = mid_input) THEN
        RETURN FALSE;
    ELSE
        RETURN TRUE;
    END IF;
END;
$$ LANGUAGE plpgsql;

--Function 6: Do user exist?
CREATE OR REPLACE FUNCTION is_user_exist(
    mid_input BIGINT
) RETURNS BOOLEAN AS $$
BEGIN
    RETURN EXISTS(SELECT 1 FROM user_info WHERE user_info.mid = mid_input);
END;
$$ LANGUAGE plpgsql;

--FUnction 7: is register info valid?
CREATE OR REPLACE FUNCTION is_reg_valid(
    name_input   varchar,
    qq_input     varchar,
    wechat_input varchar
) RETURNS INTEGER AS $$
BEGIN
    IF(EXISTS(SELECT 1 from user_info as ui 
    where ui.name = name_input
    OR ui.qq = qq_input OR ui.wechat = wechat_input)) THEN
        RETURN 0;
    ELSE
        RETURN 1;
    END IF;
END;
$$ LANGUAGE plpgsql;

--Function 8: Generate the MID
CREATE OR REPLACE FUNCTION generate_mid ()
RETURNS BIGINT AS $$
DECLARE
    max_value  BIGINT;
BEGIN
    SELECT max(ui.mid) INTO max_value FROM user_info as ui;
    RETURN max_value + 1;
END;
$$ LANGUAGE plpgsql;

--Function 9: Generate BV function:
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

--Video Service
--Function 1: Create Video
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

--User Service
CREATE OR REPLACE FUNCTION register(
    password_input  varchar,
    name_input      varchar,
    qq_input        varchar,
    wechat_input    varchar,
    gender_input    varchar,
    birthday_input  varchar,
    sign_input      varchar
) RETURNS BIGINT AS $$
DECLARE
    reg_valid INTEGER;
    mid_output BIGINT;
BEGIN
    SELECT is_reg_valid(name_input, qq_input, wechat_input) INTO reg_valid;
    IF(reg_valid = 0) THEN
        RETURN -1;
    END IF;
    SELECT generate_mid() INTO mid_output;
    INSERT INTO user_info (mid, name, gender, birthday, coin, sign, identity, password, qq, wechat)
    VALUES(mid_output, name_input, gender_input, birthday_input, 0, sign_input, 0, password_input, qq_input, wechat_input);
    RETURN mid_output;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION delete_user (
    mid_input           BIGINT,
    password_input      VARCHAR,
    qq_input            VARCHAR,
    wechat_input        VARCHAR,
    mid_deletee   BIGINT
)RETURNS INTEGER AS $$
DECLARE
    deleter_input   BIGINT;
    deleter_identity SMALLINT;
    deletee_identity SMALLINT;
BEGIN
    SELECT is_auth_valid(mid_input, password_input, qq_input, wechat_input) INTO deleter_input;
    IF deleter_input = 0 THEN
        RETURN 0;
    END IF;
    IF(NOT EXISTS(SELECT 1 FROM user_info as ui WHERE ui.mid = mid_deletee)) THEN
        RETURN 0;
    END IF;
    --Deleter is a regular user and the deletee is not himself.
    SELECT ui.identity INTO deleter_identity FROM user_info as ui WHERE ui.mid = deleter_input;
    SELECT ui.identity INTO deletee_identity FROM user_info as ui WHERE ui.mid = mid_deletee;
    IF(deleter_identity = 0 AND deleter_input != mid_deletee)
    OR(deleter_identity = 1 AND deletee_identity = 1 AND deleter_input != mid_deletee) THEN
        RETURN 0;
    ELSE
        DELETE FROM user_info as ui WHERE ui.mid = mid_deletee;
        RETURN 1;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION follow_user (
    mid_input           BIGINT,
    password_input      VARCHAR,
    qq_input            VARCHAR,
    wechat_input        VARCHAR,
    mid_followee        BIGINT
)RETURNS INTEGER AS $$
DECLARE
    mid_follower        BIGINT;
BEGIN
    SELECT is_auth_valid(mid_input, password_input, qq_input, wechat_input) INTO mid_follower;
    IF mid_follower = 0 OR mid_follower = mid_followee THEN
        RETURN 0;
    END IF;
    IF(EXISTS(SELECT 1 FROM follow WHERE follower = mid_follower AND followee = mid_followee)) THEN
        DELETE FROM follow WHERE follower = mid_follower AND followee = mid_followee;
        RETURN 0;
    ELSIF(EXISTS(SELECT 1 FROM user_info as ui WHERE ui.mid = mid_followee)) THEN
        INSERT into follow (follower, followee) values (mid_follower, mid_followee);
        RETURN 1;
    END IF;
    RETURN 0;
END;
$$ LANGUAGE plpgsql;

--Danmu Service
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

--Recommend Service
--Function 1: Recommend next video
CREATE OR REPLACE FUNCTION recommend_next_videos (
    bv_input VARCHAR
) RETURNS VARCHAR[] AS $$
DECLARE
    bv_ VARCHAR;
    bv_list VARCHAR [];
BEGIN
    --Cannot find a video corresponding to the bv
    SELECT is_bv_valid_for_users(bv_input) INTO bv_;
    IF bv_ IS NULL THEN
        RETURN '{}';
    END IF;

    SELECT ARRAY(
    SELECT w_1.bv FROM watch AS w_1
    WHERE w_1.mid IN
    (SELECT w.mid
    FROM watch AS w
    WHERE w.bv = bv_input)
    AND w_1.bv != bv_input
    GROUP BY (w_1.bv)
    ORDER BY count(*) DESC
    LIMIT 5) INTO bv_list;

    RETURN bv_list;
END;
$$ LANGUAGE plpgsql;

--Function 2: To recommend general videos for a user. 
CREATE OR REPLACE FUNCTION general_recommendation(
    page_size_input INTEGER,
    page_num_input INTEGER
) RETURNS VARCHAR [] AS $$
DECLARE
    bv_list VARCHAR [];
BEGIN
    SELECT ARRAY(SELECT bv FROM(
        SELECT watched.bv,
            CASE WHEN (watched.watch_count = 0) THEN 0 ELSE
            (CASE WHEN (liked.like_count::NUMERIC / watched.watch_count::NUMERIC) > 1 THEN 1 
            WHEN liked.like_count::NUMERIC IS NULL THEN 0
            ELSE (liked.like_count::NUMERIC / watched.watch_count::NUMERIC) END +
            CASE WHEN (coined.coin_count::NUMERIC / watched.watch_count::NUMERIC) > 1 THEN 1
            WHEN coined.coin_count::NUMERIC IS NULL THEN 0
            ELSE (coined.coin_count::NUMERIC / watched.watch_count::NUMERIC) END +
            CASE WHEN (favorited.favorite_count::NUMERIC / watched.watch_count::NUMERIC) > 1 THEN 1
            WHEN favorited.favorite_count::NUMERIC IS NULL THEN 0
            ELSE (favorited.favorite_count::NUMERIC / watched.watch_count::NUMERIC) END + 
            CASE WHEN (danmued.danmu_count::NUMERIC / watched.watch_count::NUMERIC) IS NULL THEN 0 
            ELSE (danmued.danmu_count::NUMERIC / watched.watch_count::NUMERIC) END+
            (watched.watch_duration::NUMERIC / (video.duration::NUMERIC * watched.watch_count :: NUMERIC))) END
            AS score
        FROM (SELECT watch.bv, sum(watchduration) AS watch_duration, count(*) AS watch_count FROM watch GROUP BY watch.bv) AS watched
        LEFT JOIN (SELECT like_video.bv, count(*) AS like_count FROM like_video GROUP BY like_video.bv) AS liked ON liked.bv = watched.bv
        LEFT JOIN (SELECT coin.bv, count(*) AS coin_count FROM coin GROUP BY coin.bv) AS coined ON coined.bv = watched.bv
        LEFT JOIN (SELECT favorite.bv, count(*) AS favorite_count FROM favorite GROUP BY favorite.bv) AS favorited ON favorited.bv = watched.bv
        LEFT JOIN (SELECT danmu.bv, count(*) AS danmu_count FROM danmu GROUP BY danmu.bv) AS danmued ON danmued.bv = watched.bv
        LEFT JOIN (SELECT video_info.bv, video_info.duration FROM video_info) AS video ON video.bv = watched.bv
        ORDER BY score DESC)
        LIMIT page_size_input
        OFFSET (page_num_input - 1) * page_size_input)
    INTO bv_list;
    RETURN bv_list;
END;
$$ LANGUAGE plpgsql;

--Function 3: Recommend Videos for User 
CREATE OR REPLACE FUNCTION recommend_videos_for_user(
    mid_input           BIGINT,
    password_input      VARCHAR,
    qq_input            VARCHAR,
    wechat_input        VARCHAR,
    page_size_input     INTEGER,
    page_num_input      INTEGER
) RETURNS VARCHAR [] AS $$
DECLARE
    mid_output BIGINT;
    friends    BIGINT  [];
    bv_list    VARCHAR [];
BEGIN
    SELECT is_auth_valid(mid_input, password_input, qq_input, wechat_input) INTO mid_output;
    IF  mid_output = 0 THEN
        RETURN '{}';
    END IF;
    SELECT find_friends(mid_output) INTO friends;
    IF array_length(friends, 1) IS NULL THEN
        SELECT general_recommendation(page_size_input, page_num_input) INTO bv_list;
    ELSE
        SELECT ARRAY(SELECT bv FROM 
        (SELECT unwatched.bv, user_info.level, friend_watch_count
        FROM
        (SELECT watch_.bv, count(*) AS friend_watch_count FROM watch AS watch_ 
        WHERE watch_.mid = ANY(friends) AND watch_.bv NOT IN (SELECT bv FROM watch WHERE mid = mid_output)
        GROUP BY watch_.bv) AS unwatched
        JOIN video_info ON video_info.bv = unwatched.bv
        JOIN user_info ON user_info.mid = video_info.ownermid
        ORDER BY friend_watch_count DESC, user_info.level DESC, video_info.publictime DESC
        LIMIT page_size_input
        OFFSET (page_num_input - 1) * page_size_input)) INTO bv_list;
    END IF;
    RETURN  bv_list;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION find_friends(
    mid_input BIGINT
)RETURNS BIGINT [] AS $$
BEGIN
    RETURN ARRAY(
        SELECT followee
        FROM follow
        WHERE follower = mid_input
        INTERSECT
        SELECT follower
        FROM follow
        WHERE followee = mid_input
    );
END;
$$ LANGUAGE plpgsql;

--Function 4: Recommend friends for a user
CREATE OR REPLACE FUNCTION recommend_friends_for_user(
    mid_input           BIGINT,
    password_input      VARCHAR,
    qq_input            VARCHAR,
    wechat_input        VARCHAR,
    page_size_input     INTEGER,
    page_num_input      INTEGER
) RETURNS BIGINT [] AS $$
DECLARE
    mid_output BIGINT;
    followees  BIGINT  [];
    recommend_friends BIGINT [];
BEGIN
    SELECT is_auth_valid(mid_input, password_input, qq_input, wechat_input) INTO mid_output;
    IF  mid_output = 0 THEN
        RETURN '{}';
    END IF;

    SELECT ARRAY(SELECT followee FROM follow where follower = mid_output) INTO followees ;

    SELECT ARRAY(SELECT follower FROM
    (SELECT follow.follower, un_followed.level AS level_, count(*) AS common_following 
    FROM follow JOIN
    (SELECT user_info.mid,  user_info.level FROM user_info
    WHERE user_info.mid <> ALL(followees)) AS un_followed
    ON follow.follower = un_followed.mid
    WHERE follow.followee = ANY(followees)
    AND follow.follower <> mid_output
    GROUP BY follower, level_
    ORDER BY common_following DESC, level_ DESC, follower ASC
    LIMIT page_size_input
    OFFSET (page_num_input - 1) * page_size_input)) INTO recommend_friends;
    RETURN recommend_friends;
END;
$$ LANGUAGE plpgsql;

