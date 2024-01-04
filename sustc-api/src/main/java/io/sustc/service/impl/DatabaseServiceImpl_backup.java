package io.sustc.service.impl;

import io.sustc.dto.DanmuRecord;
import io.sustc.dto.UserRecord;
import io.sustc.dto.VideoRecord;
import io.sustc.service.DatabaseService;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * It's important to mark your implementation class with {@link Service} annotation.
 * As long as the class is annotated and implements the corresponding interface, you can place it under any package.
 */
//@Service
@Slf4j
public class DatabaseServiceImpl_backup implements DatabaseService {

    /**
     * Getting a {@link DataSource} instance from the framework, whose connections are managed by HikariCP.
     * <p>
     * Marking a field with {@link Autowired} annotation enables our framework to automatically
     * provide you a well-configured instance of {@link DataSource}.
     * Learn more: <a href="https://www.baeldung.com/spring-dependency-injection">Dependency Injection</a>
     */
    @Autowired
    private DataSource dataSource;

    @Override
    public List<Integer> getGroupMembers() {
        return Arrays.asList(12210414);
    }

    @Override
    public void importData(
            List<DanmuRecord> danmuRecords,
            List<UserRecord> userRecords,
            List<VideoRecord> videoRecords
    ) {
        int batch = 50000;
        //String drop_constraint = "SELECT drop_constraints_foreign_keys()";
        //String drop_indexes = "SELECT drop_all_indexes()";
        //try(Connection conn = dataSource.getConnection();
        //    PreparedStatement constraint_stmt = conn.prepareStatement(drop_constraint);
        //    PreparedStatement index_stmt = conn.prepareStatement(drop_indexes)){
        //    constraint_stmt.executeQuery();
        //    index_stmt.executeQuery();
        //    constraint_stmt.close();
        //    index_stmt.close();
        //    System.out.println("Foreign Keys and indexes dropped");
        //}catch(SQLException e){
        //    log.debug(e.getMessage());
        //    System.out.println("Foreign Keys and indexes timeout.");
        //}
        String sql_user = "INSERT INTO user_info (mid, name, gender, birthday, level, sign, identity, password, qq, wechat, coin) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        String sql_follow = "INSERT INTO follow (follower, followee) values (?, ?)";
        String sql_video = "INSERT INTO video_info (bv, title, ownerMID, commitTime, publicTime, reviewTime, reviewerMID, duration, description) values (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        String watch_sql = "INSERT INTO Watch (bv, mid, watchduration) VALUES (?, ?, ?)";
        String sql_danmu = "INSERT INTO danmu (bv, mid, displaytime, content, posttime) VALUES (?, ?, ?, ?, ?) RETURNING Danmu_ID";
        String sql_like_danmu = "INSERT INTO like_danmu (danmu_id, mid) VALUES (?, ?)";
        String like_sql = "INSERT INTO like_video (bv, mid) VALUES (?, ?)";
        String coin_sql = "INSERT INTO Coin (bv, mid) VALUES (?, ?)";
        String favorite_sql = "INSERT INTO Favorite (bv, mid) VALUES (?, ?)";
        //Thread 1
        Thread thread_1 = new Thread(new Runnable() {
            @Override
            public void run() {
                log.info("[Thread 1] starts running.]");
                try(Connection conn = dataSource.getConnection();
                    PreparedStatement user_stmt = conn.prepareStatement(sql_user);
                    PreparedStatement follow_stmt = conn.prepareStatement(sql_follow);
                    PreparedStatement video_stmt = conn.prepareStatement(sql_video);
                    PreparedStatement watch_stmt = conn.prepareStatement(watch_sql);
                    PreparedStatement danmu_stmt = conn.prepareStatement(sql_danmu);
                    PreparedStatement like_danmu_stmt = conn.prepareStatement(sql_like_danmu);
                    PreparedStatement like_stmt = conn.prepareStatement(like_sql);
                    PreparedStatement coin_stmt = conn.prepareStatement(coin_sql);
                    PreparedStatement fav_stmt = conn.prepareStatement(favorite_sql)){
                        conn.setAutoCommit(false);
                        //User relevant information insertion: user_info and follow
                        for (int i = 0; i < userRecords.size() / 3; i++) {
                            user_stmt.setLong(1, userRecords.get(i).getMid());
                            user_stmt.setString(2, userRecords.get(i).getName());
                            user_stmt.setString(3, userRecords.get(i).getSex());
                            user_stmt.setString(4, userRecords.get(i).getBirthday());
                            user_stmt.setShort(5, userRecords.get(i).getLevel());
                            user_stmt.setString(6, userRecords.get(i).getSign());
                            user_stmt.setShort(7, (userRecords.get(i).getIdentity() == UserRecord.Identity.USER)? (short)0 : (short)1);
                            user_stmt.setString(8, userRecords.get(i).getPassword());
                            user_stmt.setString(9, userRecords.get(i).getQq());
                            user_stmt.setString(10, userRecords.get(i).getWechat());
                            user_stmt.setLong(11, userRecords.get(i).getCoin());
                            user_stmt.addBatch();
                            
                            int count = 0;
                            for (int j = 0; j < userRecords.get(i).getFollowing().length; j++) {
                                follow_stmt.setLong(1, userRecords.get(i).getMid());
                                follow_stmt.setLong(2, userRecords.get(i).getFollowing()[j]);
                                follow_stmt.addBatch();
                                count++;
                                if(count % batch == 0){
                                    follow_stmt.executeBatch();
                                }
                            }
                        }
                        user_stmt.executeBatch();
                        follow_stmt.executeBatch();
                        conn.commit();
                        user_stmt.close();
                        follow_stmt.close();
                        log.info("[Thread 1] UserRecords inserted.");

                        //Video relevant information insertion: video_info, watch, like_video, coin, favorite
                        for (int i = 0; i < videoRecords.size() / 3; i++) {
                            video_stmt.setString(1, videoRecords.get(i).getBv());
                            video_stmt.setString(2, videoRecords.get(i).getTitle());
                            video_stmt.setLong(3, videoRecords.get(i).getOwnerMid());
                            video_stmt.setTimestamp(4, videoRecords.get(i).getCommitTime());
                            video_stmt.setTimestamp(5, videoRecords.get(i).getPublicTime());
                            video_stmt.setTimestamp(6, videoRecords.get(i).getReviewTime());
                            video_stmt.setLong(7, videoRecords.get(i).getReviewer());
                            video_stmt.setFloat(8, videoRecords.get(i).getDuration());
                            video_stmt.setString(9, videoRecords.get(i).getDescription());
                            video_stmt.addBatch();
                            
                            int count = 0;
                            long [] viewers = videoRecords.get(i).getViewerMids();
                            float [] viewtime = videoRecords.get(i).getViewTime();
                            for (int j = 0; j < videoRecords.get(i).getViewerMids().length; j++) {
                                watch_stmt.setString(1, videoRecords.get(i).getBv());
                                watch_stmt.setLong(2, viewers[j]);
                                watch_stmt.setFloat(3, viewtime[j]);
                                watch_stmt.addBatch();
                                count++;
                                if(count % batch == 0){
                                    watch_stmt.executeBatch();
                                    conn.commit();
                                }
                            }
                            
                            count = 0;
                            long[] like = videoRecords.get(i).getLike();
                            for (int j = 0; j < like.length; j++) {
                                like_stmt.setString(1, videoRecords.get(i).getBv());
                                like_stmt.setLong(2, like[j]);
                                like_stmt.addBatch();
                                count++;
                                if(count % batch== 0){
                                    like_stmt.executeBatch();
                                    conn.commit();
                                }
                            }
                            
                            count = 0;
                            long[] coin = videoRecords.get(i).getCoin();
                            for (int j = 0; j < coin.length; j++) {
                                coin_stmt.setString(1, videoRecords.get(i).getBv());
                                coin_stmt.setLong(2, coin[j]);
                                coin_stmt.addBatch();
                                count++;
                                if(count % batch== 0){
                                    coin_stmt.executeBatch();
                                    conn.commit();
                                }
                            }
                            
                            count = 0;
                            long[] fav = videoRecords.get(i).getFavorite();
                            for (int j = 0; j < fav.length; j++) {
                                fav_stmt.setString(1, videoRecords.get(i).getBv());
                                fav_stmt.setLong(2, fav[j]);
                                fav_stmt.addBatch();
                                count++;
                                if(count % batch== 0){
                                    fav_stmt.executeBatch();
                                    conn.commit();
                                }
                            }
                        }
                        video_stmt.executeBatch();
                        watch_stmt.executeBatch();
                        like_stmt.executeBatch();
                        coin_stmt.executeBatch();
                        fav_stmt.executeBatch();
                        conn.commit();
                        video_stmt.close();
                        watch_stmt.close();
                        like_stmt.close();
                        coin_stmt.close();
                        fav_stmt.close();
                        log.info("[Thread 1] VideoRecords inserted.");

                        //Danmu relevant information insertion: danmu, like_danmu
                        for (int i = 0; i < danmuRecords.size() / 3; i++) {
                            danmu_stmt.setString(1, danmuRecords.get(i).getBv());
                            danmu_stmt.setLong(2, danmuRecords.get(i).getMid());
                            danmu_stmt.setFloat(3, danmuRecords.get(i).getTime());
                            danmu_stmt.setString(4, danmuRecords.get(i).getContent());
                            danmu_stmt.setTimestamp(5, danmuRecords.get(i).getPostTime());
                            ResultSet resultSet = danmu_stmt.executeQuery();
                        
                            long[] danmu_like = danmuRecords.get(i).getLikedBy();
                            long danmu_id;
                            if (resultSet.next()) {
                                danmu_id = resultSet.getLong(1);
                                for (int j = 0; j < danmu_like.length; j++) {
                                    like_danmu_stmt.setLong(1, danmu_id);
                                    like_danmu_stmt.setLong(2, danmu_like[j]);
                                    like_danmu_stmt.addBatch();
                                }
                            }else{
                                continue;
                            }
                            resultSet.close();
                        }
                        like_danmu_stmt.executeBatch();
                        danmu_stmt.executeBatch();
                        conn.commit();
                        danmu_stmt.close();
                        like_danmu_stmt.close();
                        log.info("[Thread 1] DanmuRecords inserted.");
                }catch(SQLException e){
                    log.debug(e.getMessage());
                    log.error("[Thread 1] Connection failed.");
                }
                log.info("[Thread 1] ends running.");
            }
        });

        //Thread 2 inserts video_info and watch
        Thread thread_2 = new Thread(new Runnable(){
            @Override
            public void run(){
                log.info("[Thread 2] starts running.]");
                try(Connection conn = dataSource.getConnection();
                    PreparedStatement user_stmt = conn.prepareStatement(sql_user);
                    PreparedStatement follow_stmt = conn.prepareStatement(sql_follow);
                    PreparedStatement video_stmt = conn.prepareStatement(sql_video);
                    PreparedStatement watch_stmt = conn.prepareStatement(watch_sql);
                    PreparedStatement danmu_stmt = conn.prepareStatement(sql_danmu);
                    PreparedStatement like_danmu_stmt = conn.prepareStatement(sql_like_danmu);
                    PreparedStatement like_stmt = conn.prepareStatement(like_sql);
                    PreparedStatement coin_stmt = conn.prepareStatement(coin_sql);
                    PreparedStatement fav_stmt = conn.prepareStatement(favorite_sql)){
                        conn.setAutoCommit(false);
                        //Danmu relevant information insertion: danmu, like_danmu
                        for (int i = danmuRecords.size() / 3; i < danmuRecords.size() * 2/ 3; i++) {
                            danmu_stmt.setString(1, danmuRecords.get(i).getBv());
                            danmu_stmt.setLong(2, danmuRecords.get(i).getMid());
                            danmu_stmt.setFloat(3, danmuRecords.get(i).getTime());
                            danmu_stmt.setString(4, danmuRecords.get(i).getContent());
                            danmu_stmt.setTimestamp(5, danmuRecords.get(i).getPostTime());
                            ResultSet resultSet = danmu_stmt.executeQuery();
                        
                            long[] danmu_like = danmuRecords.get(i).getLikedBy();
                            long danmu_id;
                            if (resultSet.next()) {
                                danmu_id = resultSet.getLong(1);
                                for (int j = 0; j < danmu_like.length; j++) {
                                    like_danmu_stmt.setLong(1, danmu_id);
                                    like_danmu_stmt.setLong(2, danmu_like[j]);
                                    like_danmu_stmt.addBatch();
                                }
                            }else{
                                continue;
                            }
                            resultSet.close();
                        }
                        like_danmu_stmt.executeBatch();
                        danmu_stmt.executeBatch();
                        conn.commit();
                        danmu_stmt.close();
                        like_danmu_stmt.close();
                        log.info("[Thread 2] DanmuRecords inserted.");

                       //User relevant information insertion: user_info and follow
                        for (int i = userRecords.size() / 3; i < userRecords.size() * 2/ 3; i++) {
                            user_stmt.setLong(1, userRecords.get(i).getMid());
                            user_stmt.setString(2, userRecords.get(i).getName());
                            user_stmt.setString(3, userRecords.get(i).getSex());
                            user_stmt.setString(4, userRecords.get(i).getBirthday());
                            user_stmt.setShort(5, userRecords.get(i).getLevel());
                            user_stmt.setString(6, userRecords.get(i).getSign());
                            user_stmt.setShort(7, (userRecords.get(i).getIdentity() == UserRecord.Identity.USER)? (short)0 : (short)1);
                            user_stmt.setString(8, userRecords.get(i).getPassword());
                            user_stmt.setString(9, userRecords.get(i).getQq());
                            user_stmt.setString(10, userRecords.get(i).getWechat());
                            user_stmt.setLong(11, userRecords.get(i).getCoin());
                            user_stmt.addBatch();
                            
                            int count = 0;
                            for (int j = 0; j < userRecords.get(i).getFollowing().length; j++) {
                                follow_stmt.setLong(1, userRecords.get(i).getMid());
                                follow_stmt.setLong(2, userRecords.get(i).getFollowing()[j]);
                                follow_stmt.addBatch();
                                count++;
                                if(count % batch == 0){
                                    follow_stmt.executeBatch();
                                }
                            }
                        }
                        user_stmt.executeBatch();
                        follow_stmt.executeBatch();
                        conn.commit();
                        user_stmt.close();
                        follow_stmt.close();
                        log.info("[Thread 2] UserRecords inserted.");

                        //Video relevant information insertion: video_info, watch, like_video, coin, favorite
                        for (int i = videoRecords.size() / 3; i < videoRecords.size() * 2/3; i++) {
                            video_stmt.setString(1, videoRecords.get(i).getBv());
                            video_stmt.setString(2, videoRecords.get(i).getTitle());
                            video_stmt.setLong(3, videoRecords.get(i).getOwnerMid());
                            video_stmt.setTimestamp(4, videoRecords.get(i).getCommitTime());
                            video_stmt.setTimestamp(5, videoRecords.get(i).getPublicTime());
                            video_stmt.setTimestamp(6, videoRecords.get(i).getReviewTime());
                            video_stmt.setLong(7, videoRecords.get(i).getReviewer());
                            video_stmt.setFloat(8, videoRecords.get(i).getDuration());
                            video_stmt.setString(9, videoRecords.get(i).getDescription());
                            video_stmt.addBatch();
                            
                            int count = 0;
                            long [] viewers = videoRecords.get(i).getViewerMids();
                            float [] viewtime = videoRecords.get(i).getViewTime();
                            for (int j = 0; j < videoRecords.get(i).getViewerMids().length; j++) {
                                watch_stmt.setString(1, videoRecords.get(i).getBv());
                                watch_stmt.setLong(2, viewers[j]);
                                watch_stmt.setFloat(3, viewtime[j]);
                                watch_stmt.addBatch();
                                count++;
                                if(count % batch == 0){
                                    watch_stmt.executeBatch();
                                    conn.commit();
                                }
                            }
                            
                            count = 0;
                            long[] like = videoRecords.get(i).getLike();
                            for (int j = 0; j < like.length; j++) {
                                like_stmt.setString(1, videoRecords.get(i).getBv());
                                like_stmt.setLong(2, like[j]);
                                like_stmt.addBatch();
                                count++;
                                if(count % batch== 0){
                                    like_stmt.executeBatch();
                                    conn.commit();
                                }
                            }
                            
                            count = 0;
                            long[] coin = videoRecords.get(i).getCoin();
                            for (int j = 0; j < coin.length; j++) {
                                coin_stmt.setString(1, videoRecords.get(i).getBv());
                                coin_stmt.setLong(2, coin[j]);
                                coin_stmt.addBatch();
                                count++;
                                if(count % batch== 0){
                                    coin_stmt.executeBatch();
                                    conn.commit();
                                }
                            }
                            
                            count = 0;
                            long[] fav = videoRecords.get(i).getFavorite();
                            for (int j = 0; j < fav.length; j++) {
                                fav_stmt.setString(1, videoRecords.get(i).getBv());
                                fav_stmt.setLong(2, fav[j]);
                                fav_stmt.addBatch();
                                count++;
                                if(count % batch== 0){
                                    fav_stmt.executeBatch();
                                    conn.commit();
                                }
                            }
                        }
                        video_stmt.executeBatch();
                        watch_stmt.executeBatch();
                        like_stmt.executeBatch();
                        coin_stmt.executeBatch();
                        fav_stmt.executeBatch();
                        conn.commit();
                        video_stmt.close();
                        watch_stmt.close();
                        like_stmt.close();
                        coin_stmt.close();
                        fav_stmt.close();
                        log.info("[Thread 2] VideoRecords inserted.");
                }catch(SQLException e){
                    log.debug(e.getMessage());
                    log.error("[Thread 2] Connection failed.");
                }
                log.info("[Thread 2] ends running.");
            }
        });

        //Thread 3 inserts danmu and like_danmu and like_video
        Thread thread_3 = new Thread(new Runnable(){
            @Override
            public void run(){
                log.info("[Thread 3] starts running.]");
                try(Connection conn = dataSource.getConnection();
                    PreparedStatement user_stmt = conn.prepareStatement(sql_user);
                    PreparedStatement follow_stmt = conn.prepareStatement(sql_follow);
                    PreparedStatement video_stmt = conn.prepareStatement(sql_video);
                    PreparedStatement watch_stmt = conn.prepareStatement(watch_sql);
                    PreparedStatement danmu_stmt = conn.prepareStatement(sql_danmu);
                    PreparedStatement like_danmu_stmt = conn.prepareStatement(sql_like_danmu);
                    PreparedStatement like_stmt = conn.prepareStatement(like_sql);
                    PreparedStatement coin_stmt = conn.prepareStatement(coin_sql);
                    PreparedStatement fav_stmt = conn.prepareStatement(favorite_sql)){
                        conn.setAutoCommit(false);
                        //Video relevant information insertion: video_info, watch, like_video, coin, favorite
                        for (int i = videoRecords.size() * 2/ 3; i < videoRecords.size(); i++) {
                            video_stmt.setString(1, videoRecords.get(i).getBv());
                            video_stmt.setString(2, videoRecords.get(i).getTitle());
                            video_stmt.setLong(3, videoRecords.get(i).getOwnerMid());
                            video_stmt.setTimestamp(4, videoRecords.get(i).getCommitTime());
                            video_stmt.setTimestamp(5, videoRecords.get(i).getPublicTime());
                            video_stmt.setTimestamp(6, videoRecords.get(i).getReviewTime());
                            video_stmt.setLong(7, videoRecords.get(i).getReviewer());
                            video_stmt.setFloat(8, videoRecords.get(i).getDuration());
                            video_stmt.setString(9, videoRecords.get(i).getDescription());
                            video_stmt.addBatch();
                            
                            int count = 0;
                            long [] viewers = videoRecords.get(i).getViewerMids();
                            float [] viewtime = videoRecords.get(i).getViewTime();
                            for (int j = 0; j < videoRecords.get(i).getViewerMids().length; j++) {
                                watch_stmt.setString(1, videoRecords.get(i).getBv());
                                watch_stmt.setLong(2, viewers[j]);
                                watch_stmt.setFloat(3, viewtime[j]);
                                watch_stmt.addBatch();
                                count++;
                                if(count % batch == 0){
                                    watch_stmt.executeBatch();
                                    conn.commit();
                                }
                            }
                            
                            count = 0;
                            long[] like = videoRecords.get(i).getLike();
                            for (int j = 0; j < like.length; j++) {
                                like_stmt.setString(1, videoRecords.get(i).getBv());
                                like_stmt.setLong(2, like[j]);
                                like_stmt.addBatch();
                                count++;
                                if(count % batch== 0){
                                    like_stmt.executeBatch();
                                    conn.commit();
                                }
                            }
                            
                            count = 0;
                            long[] coin = videoRecords.get(i).getCoin();
                            for (int j = 0; j < coin.length; j++) {
                                coin_stmt.setString(1, videoRecords.get(i).getBv());
                                coin_stmt.setLong(2, coin[j]);
                                coin_stmt.addBatch();
                                count++;
                                if(count % batch== 0){
                                    coin_stmt.executeBatch();
                                    conn.commit();
                                }
                            }
                            
                            count = 0;
                            long[] fav = videoRecords.get(i).getFavorite();
                            for (int j = 0; j < fav.length; j++) {
                                fav_stmt.setString(1, videoRecords.get(i).getBv());
                                fav_stmt.setLong(2, fav[j]);
                                fav_stmt.addBatch();
                                count++;
                                if(count % batch== 0){
                                    fav_stmt.executeBatch();
                                    conn.commit();
                                }
                            }
                        }
                        video_stmt.executeBatch();
                        watch_stmt.executeBatch();
                        like_stmt.executeBatch();
                        coin_stmt.executeBatch();
                        fav_stmt.executeBatch();
                        conn.commit();
                        video_stmt.close();
                        watch_stmt.close();
                        like_stmt.close();
                        coin_stmt.close();
                        fav_stmt.close();
                        log.info("[Thread 3] VideoRecords inserted.");
                        
                        //Danmu relevant information insertion: danmu, like_danmu
                        for (int i = danmuRecords.size() * 2/ 3; i < danmuRecords.size(); i++) {
                            danmu_stmt.setString(1, danmuRecords.get(i).getBv());
                            danmu_stmt.setLong(2, danmuRecords.get(i).getMid());
                            danmu_stmt.setFloat(3, danmuRecords.get(i).getTime());
                            danmu_stmt.setString(4, danmuRecords.get(i).getContent());
                            danmu_stmt.setTimestamp(5, danmuRecords.get(i).getPostTime());
                            ResultSet resultSet = danmu_stmt.executeQuery();
                        
                            long[] danmu_like = danmuRecords.get(i).getLikedBy();
                            long danmu_id;
                            if (resultSet.next()) {
                                danmu_id = resultSet.getLong(1);
                                for (int j = 0; j < danmu_like.length; j++) {
                                    like_danmu_stmt.setLong(1, danmu_id);
                                    like_danmu_stmt.setLong(2, danmu_like[j]);
                                    like_danmu_stmt.addBatch();
                                }
                            }else{
                                continue;
                            }
                            resultSet.close();
                        }
                        like_danmu_stmt.executeBatch();
                        danmu_stmt.executeBatch();
                        conn.commit();
                        danmu_stmt.close();
                        like_danmu_stmt.close();
                        log.info("[Thread 3] DanmuRecords inserted.");

                       //User relevant information insertion: user_info and follow
                        for (int i = userRecords.size() * 2/ 3; i < userRecords.size(); i++) {
                            user_stmt.setLong(1, userRecords.get(i).getMid());
                            user_stmt.setString(2, userRecords.get(i).getName());
                            user_stmt.setString(3, userRecords.get(i).getSex());
                            user_stmt.setString(4, userRecords.get(i).getBirthday());
                            user_stmt.setShort(5, userRecords.get(i).getLevel());
                            user_stmt.setString(6, userRecords.get(i).getSign());
                            user_stmt.setShort(7, (userRecords.get(i).getIdentity() == UserRecord.Identity.USER)? (short)0 : (short)1);
                            user_stmt.setString(8, userRecords.get(i).getPassword());
                            user_stmt.setString(9, userRecords.get(i).getQq());
                            user_stmt.setString(10, userRecords.get(i).getWechat());
                            user_stmt.setLong(11, userRecords.get(i).getCoin());
                            user_stmt.addBatch();
                            
                            int count = 0;
                            for (int j = 0; j < userRecords.get(i).getFollowing().length; j++) {
                                follow_stmt.setLong(1, userRecords.get(i).getMid());
                                follow_stmt.setLong(2, userRecords.get(i).getFollowing()[j]);
                                follow_stmt.addBatch();
                                count++;
                                if(count % batch == 0){
                                    follow_stmt.executeBatch();
                                }
                            }
                        }
                        user_stmt.executeBatch();
                        follow_stmt.executeBatch();
                        conn.commit();
                        user_stmt.close();
                        follow_stmt.close();
                        log.info("[Thread 3] UserRecords inserted.");

                        
                }catch(SQLException e){
                    log.debug(e.getMessage());
                    log.error("[Thread 3] Connection failed.");
                }
                log.info("[Thread 3] ends running.");
            }
        });
        thread_1.start();
        thread_2.start();
        thread_3.start();
        try{
            thread_1.join();
            thread_2.join();
            thread_3.join();
        }catch(InterruptedException e){
            System.out.println("Thread interrupted!");
        }
        //String add_constraint = "SELECT add_constraints_foreign_keys()";
        //String add_indexes = "SELECT add_all_indexes()";
        //try(Connection conn = dataSource.getConnection();
        //    PreparedStatement constraint_stmt = conn.prepareStatement(add_constraint);
        //    PreparedStatement index_stmt = conn.prepareStatement(add_indexes)){
        //    constraint_stmt.executeQuery();
        //    index_stmt.executeQuery();
        //    constraint_stmt.close();
        //    index_stmt.close();
        //    System.out.println("Foreign keys and indexes created");
        //}catch(SQLException e){
        //    log.debug(e.getMessage());
        //    System.out.println("Constraint Insertion failed.");
        //}
    }    
    /*
     * The following code is just a quick example of using jdbc datasource.
     * Practically, the code interacts with database is usually written in a DAO layer.
     *
     * Reference: [Data Access Object pattern](https://www.baeldung.com/java-dao-pattern)
     */

    @Override
    public void truncate() {
        // You can use the default truncate script provided by us in most cases,
        // but if it doesn't work properly, you may need to modify it.

        String sql = "DO $$\n" +
                "DECLARE\n" +
                "    tables CURSOR FOR\n" +
                "        SELECT tablename\n" +
                "        FROM pg_tables\n" +
                "        WHERE schemaname = 'public';\n" +
                "BEGIN\n" +
                "    FOR t IN tables\n" +
                "    LOOP\n" +
                "        EXECUTE 'TRUNCATE TABLE ' || QUOTE_IDENT(t.tablename) || ' CASCADE;';\n" +
                "    END LOOP;\n" +
                "END $$;\n";

        try (Connection conn = dataSource.getConnection();
            PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Integer sum(int a, int b) {
        String sql = "SELECT ?+?";

        try (Connection conn = dataSource.getConnection();
            PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, a);
            stmt.setInt(2, b);
            log.info("SQL: {}", stmt);

            ResultSet rs = stmt.executeQuery();
            rs.next();
            return rs.getInt(1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}

