-- Active: 1698991389422@@127.0.0.1@5432@sustc@public
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
    wechat          VARCHAR             
    CONSTRAINT pk_user_info_xiaoyc
        PRIMARY KEY(MID)
);
        
--Table Follow
CREATE TABLE Follow (
    follower        BIGINT,
    followee        BIGINT
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
    description     TEXT
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
    postTime        TIMESTAMP
    CONSTRAINT  pk_danmu_xiaoyc
        PRIMARY KEY (Danmu_ID)
);

--Table watch
CREATE TABLE Watch (
    BV              VARCHAR(255),
    MID             BIGINT,
    watchDuration   NUMERIC
);

--Table Like_video
CREATE TABLE Like_video (
    BV              VARCHAR(255),
    MID             BIGINT
);

--Table Coin
CREATE TABLE Coin (
    BV              VARCHAR(255),
    MID             BIGINT
);

--Table Favorite
CREATE TABLE Favorite (
    BV              VARCHAR(255),
    MID             BIGINT
);

--Table Like Danmu
CREATE TABLE Like_danmu (
    Danmu_id        BIGINT,
    MID             BIGINT
);
--Create audit table for user_info
CREATE TABLE audit_log (
    audit_log_id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    username VARCHAR(255),
    action_type VARCHAR(50), -- e.g.UPDATE, DELETE
    table_name VARCHAR(255),
    query_text TEXT,
    client_ip VARCHAR(50),
    user_agent VARCHAR(255)
);

CREATE OR REPLACE FUNCTION audit_trigger_function()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO audit_log (username, action_type, table_name, query_text, client_ip, user_agent)
    VALUES (
        current_user,
        TG_OP,
        TG_TABLE_NAME,
        current_query(),
        inet_client_addr(),
        inet_client_port()
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

SELECT
    trigger_name,
    event_object_table,
    event_manipulation,
    action_statement
FROM information_schema.triggers
WHERE trigger_schema = 'public'; 

CREATE TRIGGER audit_trigger_user_info
AFTER UPDATE OR DELETE ON user_info
FOR EACH ROW EXECUTE FUNCTION audit_trigger_function();

DROP TRIGGER audit_trigger_user_info;

CREATE TRIGGER audit_trigger_video_info
AFTER UPDATE OR DELETE ON video_info
FOR EACH ROW EXECUTE FUNCTION audit_trigger_function();

CREATE TRIGGER audit_trigger_coin
AFTER UPDATE OR DELETE ON coin
FOR EACH ROW EXECUTE FUNCTION audit_trigger_function();

CREATE TRIGGER audit_trigger_danmu
AFTER UPDATE OR DELETE ON danmu
FOR EACH ROW EXECUTE FUNCTION audit_trigger_function();

CREATE TRIGGER audit_trigger_favorite
AFTER UPDATE OR DELETE ON favorite
FOR EACH ROW EXECUTE FUNCTION audit_trigger_function();

CREATE TRIGGER audit_trigger_follow
AFTER UPDATE OR DELETE ON follow
FOR EACH ROW EXECUTE FUNCTION audit_trigger_function();

CREATE TRIGGER audit_trigger_like_danmu
AFTER UPDATE OR DELETE ON like_danmu
FOR EACH ROW EXECUTE FUNCTION audit_trigger_function();

CREATE TRIGGER audit_trigger_like_video
AFTER UPDATE OR DELETE ON like_video
FOR EACH ROW EXECUTE FUNCTION audit_trigger_function();

CREATE TRIGGER audit_trigger_watch
AFTER UPDATE OR DELETE ON watch
FOR EACH ROW EXECUTE FUNCTION audit_trigger_function();

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
