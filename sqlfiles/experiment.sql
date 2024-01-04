-- Active: 1698991389422@@127.0.0.1@5432@sustc@public
select pg_size_pretty(pg_table_size('user_info'));
select pg_size_pretty(pg_indexes_size('user_info'));
select pg_size_pretty(pg_total_relation_size('user_info'));

select pg_size_pretty(pg_table_size('video_info'));
select pg_size_pretty(pg_indexes_size('video_info'));
select pg_size_pretty(pg_total_relation_size('video_info'));

select pg_size_pretty(pg_table_size('danmu'));
select pg_size_pretty(pg_indexes_size('danmu'));
select pg_size_pretty(pg_total_relation_size('danmu'));

select pg_size_pretty(pg_table_size('coin'));
select pg_size_pretty(pg_indexes_size('coin'));
select pg_size_pretty(pg_total_relation_size('coin'));

select pg_size_pretty(pg_table_size('like_video'));
select pg_size_pretty(pg_indexes_size('like_video'));
select pg_size_pretty(pg_total_relation_size('like_video'));

select pg_size_pretty(pg_table_size('follow'));
select pg_size_pretty(pg_indexes_size('follow'));
select pg_size_pretty(pg_total_relation_size('follow'));

select pg_size_pretty(pg_table_size('like_danmu'));
select pg_size_pretty(pg_indexes_size('like_danmu'));
select pg_size_pretty(pg_total_relation_size('like_danmu'));

select pg_size_pretty(pg_table_size('favorite'));
select pg_size_pretty(pg_indexes_size('favorite'));
select pg_size_pretty(pg_total_relation_size('favorite'));

select pg_size_pretty(pg_table_size('watch'));
select pg_size_pretty(pg_indexes_size('watch'));
select pg_size_pretty(pg_total_relation_size('watch'));

