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
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

/**
 * It's important to mark your implementation class with {@link Service} annotation.
 * As long as the class is annotated and implements the corresponding interface, you can place it under any package.
 */
@Service
@Slf4j
public class DatabaseServiceImpl implements DatabaseService {

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
        System.out.println(danmuRecords.size());
        System.out.println(userRecords.size());
        System.out.println(videoRecords.size());
        int batch = 50000;
        //Drop Constraints
        try(Connection conn = dataSource.getConnection()){
            String disable_constraints = "SELECT drop_constraints_foreign_keys()";
            long startTime = System.currentTimeMillis();
            try (PreparedStatement stmt_disable = conn.prepareStatement(disable_constraints)){
                stmt_disable.executeQuery();
            } catch (SQLException e) {
                e.printStackTrace(); 
            }
            long endTime = System.currentTimeMillis();
            System.out.println("The disable constraints time is " + (endTime - startTime) + " ms");
        }catch(SQLException e){
            System.out.println("Connection Timeout!");
        }
        //Thread 1 insert user_info and follow
        Thread thread_1 = new Thread(new Runnable() {
            @Override
            public void run() {
                System.out.println("Thread 1 starts running.");
                try(Connection conn_1 = dataSource.getConnection()){
                    conn_1.setAutoCommit(false);
                    //Import User Records
                    String sql_user = "INSERT INTO user_info (mid, name, gender, birthday, level, sign, identity, password, qq, wechat) values";
                    sql_user += "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                    long userInsertStartTime = System.currentTimeMillis();
                    try(PreparedStatement user_stmt = conn_1.prepareStatement(sql_user)){
                        for (int i = 0; i < userRecords.size(); i++) {
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
                            user_stmt.addBatch();
                        }
                    user_stmt.executeBatch();
                    conn_1.commit();
                    user_stmt.close();
                    }catch(SQLException e){
                        System.out.println("[Thread 1] User_info Insertion failed.");
                    }
                    long userInsertEndTime = System.currentTimeMillis();
                    System.out.println("[Thread 1] User_info insertion time is " + (userInsertEndTime - userInsertStartTime) + " ms");

                    //Import Follow Records
                    String sql_follow = "INSERT INTO follow (follower, followee) values";
                    sql_follow += "(?, ?)";
                    long userFollowStartTime = System.currentTimeMillis();
                    int count = 0;
                    try(PreparedStatement follow_stmt = conn_1.prepareStatement(sql_follow)){
                        for (int i = 0; i < userRecords.size(); i++) {
                            for (int j = 0; j < userRecords.get(i).getFollowing().length; j++) {
                                follow_stmt.setLong(1, userRecords.get(i).getMid());
                                follow_stmt.setLong(2, userRecords.get(i).getFollowing()[j]);
                                follow_stmt.addBatch();
                                count++;
                                if(count % batch == 0){
                                    follow_stmt.executeBatch();
                                    conn_1.commit();
                                    System.out.println("[Thread 1] " + count + " records inserted.");
                                }
                            }
                        }
                        follow_stmt.executeBatch();
                        conn_1.commit();
                        follow_stmt.close();
                    }catch(SQLException e){
                        System.out.println("[Thread 1] Follow Insertion failed.");
                    }
                    long userFollowEndTime = System.currentTimeMillis();
                    System.out.println("[Thread 1] Follow insertion time is " + (userFollowEndTime - userFollowStartTime) + " ms");

                }catch (SQLException e) {
                    System.out.println("Connection 1 timeout!");
                }
                System.out.println("Thread 1 ends running.");
            }
        });

        //Thread 2 inserts video_info and watch
        Thread thread_2 = new Thread(new Runnable(){
            @Override
            public void run(){
                System.out.println("Thread 2 starts running.");
                try(Connection conn_2 = dataSource.getConnection()){
                    conn_2.setAutoCommit(false);
                    //Import Video Records
                    String sql_video = "INSERT INTO video_info (bv, title, ownerMID, commitTime, publicTime, reviewTime, reviewerMID, duration, description) values";
                    sql_video += "(?, ?, ?, ?, ?, ?, ?, ?, ?)";
                    long videoInsertStartTime = System.currentTimeMillis();
                     try(PreparedStatement video_stmt = conn_2.prepareStatement(sql_video)){
                        for (int i = 0; i < videoRecords.size(); i++) {
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
                        }
                        video_stmt.executeBatch();
                        conn_2.commit();
                        video_stmt.close();
                    }catch(SQLException e){
                        System.out.println("[Thread 2] Video Insertion failed.");
                    }
                    long videoInsertEndTime = System.currentTimeMillis();
                    System.out.println("[Thread 2] The video insertion time is " + (videoInsertEndTime - videoInsertStartTime) + " ms");

                    //Watch Relation Insertion
                    String watch_sql = "INSERT INTO Watch (bv, mid, watchduration) VALUES";
                    watch_sql += "(?, ?, ?)";
                    long watchInsertStartTime = System.currentTimeMillis();
                    int count = 0;
                    try(PreparedStatement watch_stmt = conn_2.prepareStatement(watch_sql)){
                        for (int i = 0; i < videoRecords.size(); i++) {
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
                                    conn_2.commit();
                                    System.out.println("[Thread 2] " + count + " records inserted.");
                                }
                            }
                        }
                        watch_stmt.executeBatch(); 
                        conn_2.commit();
                        watch_stmt.close();
                    }catch(SQLException e){
                        System.out.println("[Thread 2] Watch Insertion failed.");
                    }
                    long watchInsertEndTime = System.currentTimeMillis();
                    System.out.println("[Thread 2] The watch insertion time is " + (watchInsertEndTime - watchInsertStartTime) + " ms");

                }catch(SQLException e){
                    System.out.println("Connection 2 timeout!");
                }
                System.out.println("Thread 2 ends running.");
            }
        });

        //Thread 3 inserts danmu and like_danmu and like_video
        Thread thread_3 = new Thread(new Runnable(){
            @Override
            public void run(){
                System.out.println("Thread 3 starts running.");
                try(Connection conn_3 =dataSource.getConnection()) {
                    conn_3.setAutoCommit(false);
                    //Import Danmu Records
                    String sql_danmu = "INSERT INTO danmu (bv, mid, displaytime, content, posttime) VALUES (?, ?, ?, ?, ?) RETURNING Danmu_ID";
                    String sql_like_danmu = "INSERT INTO like_danmu (Danmu_id, mid) VALUES (?, ?)";
                    long danmuInsertStartTime = System.currentTimeMillis();            
                    try(PreparedStatement danmu_stmt = conn_3.prepareStatement(sql_danmu);
                        PreparedStatement like_danmu_stmt = conn_3.prepareStatement(sql_like_danmu)) {
                        for (int i = 0; i < danmuRecords.size(); i++) {
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
                        conn_3.commit();
                        danmu_stmt.close();
                        like_danmu_stmt.close();
                    }catch (SQLException e) {
                        System.out.println("[Thread 3] Danmu Insertion failed.");
                    }
                    long danmuInsertEndTime = System.currentTimeMillis();
                    System.out.println("[Thread 3] The danmu insertion time is " + (danmuInsertEndTime - danmuInsertStartTime) + " ms");
                    
                    //Like Video Insertion
                    String like_sql = "INSERT INTO Like_video (bv, mid) VALUES";
                    like_sql += "(?, ?)";
                    long likeInsertStartTime = System.currentTimeMillis();
                    int count = 0;
                    try(PreparedStatement like_stmt = conn_3.prepareStatement(like_sql)){
                        for (int i = 0; i < videoRecords.size(); i++) {
                            long[] like = videoRecords.get(i).getLike();
                            for (int j = 0; j < like.length; j++) {
                                like_stmt.setString(1, videoRecords.get(i).getBv());
                                like_stmt.setLong(2, like[j]);
                                like_stmt.addBatch();
                                count++;
                                if(count % batch== 0){
                                    like_stmt.executeBatch();
                                    conn_3.commit();
                                    System.out.println("[Thread 3] " + count + " records inserted.");
                                }
                            }
                        }
                        like_stmt.executeBatch();
                        conn_3.commit();
                        like_stmt.close();
                    }catch(SQLException e){
                        System.out.println("[Thread 3] Like Video Insertion failed.");
                    }
                    long likeInsertEndTime = System.currentTimeMillis();
                    System.out.println("[Thread 3] The like insertion time is " + (likeInsertEndTime - likeInsertStartTime) + " ms");
                }catch(SQLException e){
                   System.out.println("Connection 3 timeout!");
                }
                System.out.println("Thread 3 ends running.");
            }
        });

        //Thread 4 inserts coin and favorite
        Thread thread_4 = new Thread(new Runnable(){
            @Override
            public void run(){
                System.out.println("Thread 4 starts running.");
                try(Connection conn_4 = dataSource.getConnection()){
                    conn_4.setAutoCommit(false);
                    //Import Coin/Favorite Records
                    String coin_sql = "INSERT INTO Coin (bv, mid) VALUES";
                    coin_sql += "(?, ?)";
                    String favorite_sql = "INSERT INTO Favorite (bv, mid) VALUES";
                    favorite_sql += "(?, ?)";
                    long coinInsertStartTime = System.currentTimeMillis();
                    try(PreparedStatement coin_stmt = conn_4.prepareStatement(coin_sql);
                        PreparedStatement fav_stmt = conn_4.prepareStatement(favorite_sql)){
                        for (int i = 0; i < videoRecords.size(); i++) {
                            long[] coin = videoRecords.get(i).getCoin();
                            for (int j = 0; j < coin.length; j++) {
                                coin_stmt.setString(1, videoRecords.get(i).getBv());
                                coin_stmt.setLong(2, coin[j]);
                                coin_stmt.addBatch();
                            }
                            long[] fav = videoRecords.get(i).getFavorite();
                            for (int j = 0; j < fav.length; j++) {
                                fav_stmt.setString(1, videoRecords.get(i).getBv());
                                fav_stmt.setLong(2, fav[j]);
                                fav_stmt.addBatch();
                            }
                            
                        }
                        coin_stmt.executeBatch();
                        fav_stmt.executeBatch();
                        conn_4.commit();
                        coin_stmt.close();
                        fav_stmt.close();
                    }catch(SQLException e){
                        System.out.println("[Thread 4] Coin/Fav Insertion failed.");
                    }
                    long coinInsertEndTime = System.currentTimeMillis();
                    System.out.println("[Thread 4] The coin/fav insertion time is " + (coinInsertEndTime - coinInsertStartTime) + " ms");
                }catch(SQLException e){
                    System.out.println("Connection 4 timeout!");
                }
                System.out.println("Thread 4 ends running.");
            }
        });
        thread_1.start();
        thread_2.start();
        thread_3.start();
        thread_4.start();
        try{
            thread_1.join();
            thread_2.join();
            thread_3.join();
            thread_4.join();
        }catch(InterruptedException e){
            System.out.println("Thread interrupted!");
        }
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
