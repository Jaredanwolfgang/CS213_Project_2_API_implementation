-- Active: 1698991389422@@127.0.0.1@5432@sustc@public
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

