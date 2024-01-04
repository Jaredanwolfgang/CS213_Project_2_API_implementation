--The Following is the method required to use.

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
    password_input := encode(digest(password_input, 'sha256'), 'hex');
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
SELECT is_bv_valid('BV13Y4y1n76d');
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
    IF EXISTS(SELECT 1 FROM video_info_view WHERE video_info_view.title = title_input AND video_info_view.ownerMID = mid_input) THEN
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

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE OR REPLACE FUNCTION encrypt_password()
RETURNS TRIGGER AS $$
BEGIN
    -- Encrypt the password using SHA256
    NEW.password := encode(digest(NEW.password, 'sha256'), 'hex');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER before_user_insert
    BEFORE INSERT ON user_info
    FOR EACH ROW
    EXECUTE FUNCTION encrypt_password();

SELECT version();
