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

EXPLAIN ANALYSE SELECT watched.bv,
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
        ORDER BY score DESC;

EXPLAIN ANALYSE SELECT general_recommendation(20, 1);

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

EXPLAIN ANALYSE SELECT followee
        FROM follow
        WHERE follower = 917516
        INTERSECT
        SELECT follower
        FROM follow
        WHERE followee = 917516;

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

SELECT * FROM user_info WHERE mid = 1410722;--(elk3ryr8G
SELECT recommend_friends_for_user(1410722, '(elk3ryr8G', '', '', 20, 1);

SELECT follow.follower, un_followed.level AS level_, count(*) AS common_following 
    FROM follow JOIN
    (SELECT user_info.mid,  user_info.level FROM user_info
    WHERE user_info.mid <> ALL(ARRAY(SELECT followee FROM follow where follower = 1410722))) AS un_followed
    ON follow.follower = un_followed.mid
    WHERE follow.followee = ANY(ARRAY(SELECT followee FROM follow where follower = 1410722))
    AND follow.follower <> 1410722
    GROUP BY follower, level_
    ORDER BY common_following DESC, level_ DESC, follower ASC;


SELECT followee
FROM follow WHERE follower = 1410722;
INTERSECT
SELECT followee
FROM follow WHERE follower = 214353;