-- Active: 1698991389422@@127.0.0.1@5432@sustc@public
CREATE OR REPLACE VIEW user_info_view AS
    SELECT * FROM user_info WHERE visible = true;

CREATE OR REPLACE VIEW video_info_view AS
    SELECT * FROM video_info WHERE visible = true;

CREATE OR REPLACE VIEW danmu_view AS
    SELECT * FROM danmu WHERE visible = true;

CREATE OR REPLACE VIEW coin_view AS
    SELECT * FROM coin WHERE visible = true;

CREATE OR REPLACE VIEW like_video_view AS
    SELECT * FROM like_video WHERE visible = true;

CREATE OR REPLACE VIEW follow_view AS
    SELECT * FROM follow WHERE visible = true;

CREATE OR REPLACE VIEW like_danmu_view AS
    SELECT * FROM like_danmu WHERE visible = true;

CREATE OR REPLACE VIEW favorite_view AS
    SELECT * FROM favorite WHERE visible = true;

CREATE OR REPLACE VIEW watch_view AS
    SELECT * FROM watch WHERE visible = true;


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


create or replace function update_watch_rate() returns trigger as $$ 
declare
begin 
  refresh materialized view concurrently dnh_analasis_view; 
  return null; 
end; 
$$ language plpgsql;

create trigger watch_rate_trigger_video_info
after insert or update or delete on video_info
for each statement 
execute procedure update_watch_rate();

create trigger watch_rate_trigger_watch
after insert or update or delete on watch
for each statement 
execute procedure update_watch_rate();
