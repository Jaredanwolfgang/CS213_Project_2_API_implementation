--The following is for creating the tables. 
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

CREATE OR REPLACE VIEW user_info_view AS
        SELECT * FROM user_info WHERE visible = true;

CREATE index user_index ON user_info (MID);
CREATE index user_index_qq ON user_info (QQ);
CREATE index user_index_wechat ON user_info (Wechat);
        
--Table Follow
CREATE TABLE Follow (
    follower        BIGINT,
    followee        BIGINT,
    visible         BOOLEAN             NOT NULL DEFAULT TRUE,
    CONSTRAINT  pk_follow_xiaoyc
        PRIMARY KEY (follower, followee)
);

CREATE OR REPLACE VIEW follow_view AS
        SELECT * FROM follow WHERE visible = true;

CREATE index follow_index ON follow (follower, followee);
CREATE index follow_index_followee ON follow (followee);

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

CREATE OR REPLACE VIEW video_info_view AS
        SELECT * FROM video_info WHERE visible = true;

CREATE index video_index ON video_info (BV);
CREATE index video_index_owner ON video_info (ownerMID);

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

CREATE OR REPLACE VIEW danmu_view AS
        SELECT * FROM danmu WHERE visible = true;

CREATE index danmu_index ON danmu (Danmu_ID, BV, MID);
CREATE index danmu_index_bv ON danmu (BV, MID);
CREATE index danmu_index_MID ON danmu (MID);

--Table watch
CREATE TABLE Watch (
    BV              VARCHAR(255),
    MID             BIGINT,
    watchDuration   NUMERIC,
    visible         BOOLEAN         NOT NULL DEFAULT TRUE
);
CREATE OR REPLACE VIEW watch_view AS
        SELECT * FROM watch WHERE visible = true;

CREATE index watch_index ON watch (BV, MID);
CREATE index watch_index_mid ON watch (MID);

--Table Like_video
CREATE TABLE Like_video (
    BV              VARCHAR(255),
    MID             BIGINT,
    visible         BOOLEAN         NOT NULL DEFAULT TRUE
);
CREATE OR REPLACE VIEW like_video_view AS
        SELECT * FROM like_video WHERE visible = true;

CREATE index like_video_index ON like_video (BV, MID);
CREATE index like_video_index_mid ON like_video (MID);

--Table Coin
CREATE TABLE Coin (
    BV              VARCHAR(255),
    MID             BIGINT,
    visible         BOOLEAN         NOT NULL DEFAULT TRUE
);
CREATE OR REPLACE VIEW coin_view AS
        SELECT * FROM coin WHERE visible = true;

CREATE index coin_index ON coin (BV, MID);
CREATE index coin_index_mid ON coin (MID);

--Table Favorite
CREATE TABLE Favorite (
    BV              VARCHAR(255),
    MID             BIGINT,
    visible         BOOLEAN         NOT NULL DEFAULT TRUE
);

CREATE OR REPLACE VIEW favorite_view AS
        SELECT * FROM favorite WHERE visible = true;

CREATE index favorite_index ON favorite (BV, MID);
CREATE index favorite_index_mid ON favorite (MID);

--Table Like Danmu
CREATE TABLE Like_danmu (
    Danmu_id        BIGINT,
    MID             BIGINT,
    visible         BOOLEAN         NOT NULL DEFAULT TRUE
);

CREATE OR REPLACE VIEW like_danmu_view AS
        SELECT * FROM like_danmu WHERE visible = true;

CREATE index like_danmu_index ON like_danmu (Danmu_id, MID);
CREATE index like_danmu_index_mid ON like_danmu (MID);

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

SELECT drop_constraints_foreign_keys();

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

SELECT add_constraints_foreign_keys();

--Drop all INDEXES
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

SELECT drop_all_indexes();

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

SELECT add_all_indexes();
