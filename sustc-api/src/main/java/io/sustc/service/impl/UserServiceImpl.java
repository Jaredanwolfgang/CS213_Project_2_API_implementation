package io.sustc.service.impl;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;
import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.RegisterUserReq;
import io.sustc.dto.UserInfoResp;
import io.sustc.dto.RegisterUserReq.Gender;
import io.sustc.service.UserService;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public  class UserServiceImpl implements UserService{
    @Autowired
    private DataSource dataSource;
        /**
     * Registers a new user.
     * {@code password} is a mandatory field, while {@code qq} and {@code wechat} are optional
     * <a href="https://openid.net/developers/how-connect-works/">OIDC</a> fields.
     *
     * @param req information of the new user
     * @return the new user's {@code mid}
     * @apiNote You may consider the following corner cases:
     * <ul>
     *   <li>{@code password} or {@code name} or {@code sex} in {@code req} is null or empty</li>
     *   <li>{@code birthday} in {@code req} is valid (not null nor empty) while it's not a birthday (X月X日)</li>
     *   <li>there is another user with same {@code name} or {@code qq} or {@code wechat} in {@code req}</li>
     * </ul>
     * If any of the corner case happened, {@code -1} shall be returned.
     */
    public long register(RegisterUserReq req){
        if(req.getPassword()==null||req.getName()==null||req.getSex()==null){
            return -1;
        }
        if(req.getBirthday()!=null&&!isValidDate(req.getBirthday(), "MM月dd日")){
            return -1;
        }
        String register = "SELECT register(?,?,?,?,?,?,?)";
        try(Connection conn = dataSource.getConnection()){
            try(PreparedStatement stmt = conn.prepareStatement(register)){
                stmt.setString(1, req.getPassword());
                stmt.setString(2, req.getName());
                stmt.setString(3, req.getQq());
                stmt.setString(4, req.getWechat());
                stmt.setString(5, req.getSex() == Gender.MALE ? "男" : (req.getSex() == Gender.FEMALE ? "女" : "保密"));
                stmt.setString(6, req.getBirthday());
                stmt.setString(7, req.getSign());
                ResultSet resultSet = stmt.executeQuery();
                long mid = -1;
                while(resultSet.next()){
                    mid = resultSet.getLong(1);
                }
                return mid;
            }catch(SQLException e){
                e.getStackTrace();
            }
        } catch (SQLException e) {
            System.out.println("Connection failed");
        }
        return -1;
    };

    public static boolean isValidDate(String dateString, String format) {
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        sdf.setLenient(false);
        try {
            Date date = sdf.parse(dateString);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
    /**
     * Deletes a user.
     *
     * @param auth indicates the current user
     * @param mid  the user to be deleted
     * @return operation success or not
     * @apiNote You may consider the following corner cases:
     * <ul>
     *   <li>cannot find a user corresponding to the {@code mid}</li>
     *   <li>the {@code auth} is invalid
     *     <ul>
     *       <li>both {@code qq} and {@code wechat} are non-empty while they do not correspond to same user</li>
     *       <li>{@code mid} is invalid while {@code qq} and {@code wechat} are both invalid (empty or not found)</li>
     *     </ul>
     *   </li>
     *   <li>the current user is a regular user while the {@code mid} is not his/hers</li>
     *   <li>the current user is a super user while the {@code mid} is neither a regular user's {@code mid} nor his/hers</li>
     * </ul>
     * If any of the corner case happened, {@code false} shall be returned.
     */
    public boolean deleteAccount(AuthInfo auth, long mid){
        String delete = "SELECT delete_user(?, ?, ?, ?, ?)";
        try (Connection conn = dataSource.getConnection();
            PreparedStatement stmt = conn.prepareStatement(delete)){
                stmt.setLong(1, auth.getMid());
                stmt.setString(2,auth.getPassword());
                stmt.setString(3, auth.getQq());
                stmt.setString(4, auth.getWechat());
                stmt.setLong(5, mid);
                ResultSet resultSet = stmt.executeQuery();
                while(resultSet.next()){
                    if(resultSet.getLong(1) == 0){
                        return false;
                    }
                }
                stmt.close();
                return true;
        } catch (Exception e) {
            log.debug(e.getMessage());
            System.out.println("Connection failed.");
        }
        return false;
    };

    /**
     * Follow the user with {@code mid}.
     * If that user has already been followed, unfollow the user.
     *
     * @param auth        the authentication information of the follower
     * @param followeeMid the user who will be followed
     * @return the follow state after this operation
     * @apiNote You may consider the following corner cases:
     * <ul>
     *   <li>{@code auth} is invalid, as stated in {@link io.sustc.service.UserService#deleteAccount(AuthInfo, long)}</li>
     *   <li>cannot find a user corresponding to the {@code followeeMid}</li>
     * </ul>
     * If any of the corner case happened, {@code false} shall be returned.
     */
    public boolean follow(AuthInfo auth, long followeeMid){
        try (Connection conn = dataSource.getConnection()) {
            String follow = "SELECT follow_user(?, ?, ?, ?, ?)";
            try(PreparedStatement stmt = conn.prepareStatement(follow)){
                stmt.setLong(1, auth.getMid());
                stmt.setString(2, auth.getPassword());
                stmt.setString(3, auth.getQq());
                stmt.setString(4, auth.getWechat());
                stmt.setLong(5, followeeMid);
                ResultSet resultSet = stmt.executeQuery();
                while(resultSet.next()){
                    if(resultSet.getLong(1) == 0){
                        return false;
                    }
                }
                stmt.close();
                return true;
            }catch (SQLException e){
                System.out.println("Failed following.");
            }
        } catch (Exception e) {
            System.out.println("Connection failed.");
        }
        return false;
    };

    /**
     * Gets the required information (in DTO) of a user.
     *
     * @param mid the user to be queried
     * @return the personal information of given {@code mid}
     * @apiNote You may consider the following corner cases:
     * <ul>
     *   <li>cannot find a user corresponding to the {@code mid}</li>
     * </ul>
     * If any of the corner case happened, {@code null} shall be returned.
     */
    public UserInfoResp getUserInfo(long mid){
        UserInfoResp userInfoResp = new UserInfoResp();
        String validation = "SELECT is_user_exist(?)";
        String coin = "SELECT user_info.coin FROM user_info WHERE user_info.mid = ?";
        String follow = "SELECT follower, followee FROM follow WHERE follow.follower = ? OR follow.followee = ?";
        String watch = "SELECT ARRAY_AGG(watch.bv) FROM watch WHERE watch.mid = ?";
        String like = "SELECT ARRAY_AGG(like_video.bv) FROM like_video WHERE like_video.mid = ?";
        String collect = "SELECT ARRAY_AGG(favorite.bv) FROM favorite WHERE favorite.mid = ?";
        String post = "SELECT ARRAY_AGG(video_info.bv) FROM video_info WHERE video_info.ownermid = ?";
        try (Connection conn = dataSource.getConnection();
            PreparedStatement valid_stmt = conn.prepareStatement(validation);
            PreparedStatement coin_stmt = conn.prepareStatement(coin);
            PreparedStatement follow_stmt = conn.prepareStatement(follow);
            PreparedStatement watch_stmt = conn.prepareStatement(watch);
            PreparedStatement like_stmt = conn.prepareStatement(like);
            PreparedStatement collect_stmt = conn.prepareStatement(collect);
            PreparedStatement post_stmt = conn.prepareStatement(post)){
            
            valid_stmt.setLong(1, mid);
            ResultSet valid_resultSet = valid_stmt.executeQuery();
            while(valid_resultSet.next()){
                if(!valid_resultSet.getBoolean(1)){
                    return null;
                }
            }
            valid_stmt.close();
            //log.info("User validation succeeded.");

            coin_stmt.setLong(1, mid);
            follow_stmt.setLong(1, mid);
            follow_stmt.setLong(2, mid);
            watch_stmt.setLong(1, mid);
            like_stmt.setLong(1, mid);
            collect_stmt.setLong(1, mid);
            post_stmt.setLong(1, mid);

            ResultSet coin_resultSet = coin_stmt.executeQuery();
            ResultSet follow_resultSet = follow_stmt.executeQuery();
            ResultSet watch_resultSet = watch_stmt.executeQuery();
            ResultSet like_resultSet = like_stmt.executeQuery();
            ResultSet collect_resultSet = collect_stmt.executeQuery();
            ResultSet post_resultSet = post_stmt.executeQuery();

            int coin_output = 0;
            while(coin_resultSet.next()){
                coin_output = coin_resultSet.getInt(1);
            }
            ArrayList<Long> followee_output = new ArrayList<Long>();
            ArrayList<Long> follower_output = new ArrayList<Long>();
            while(follow_resultSet.next()){
                if(follow_resultSet.getLong(1) == mid){
                    followee_output.add(follow_resultSet.getLong(2));
                }else{
                    follower_output.add(follow_resultSet.getLong(1));
                }
            }
            long[] following = followee_output.stream()
                                            .mapToLong(Long::longValue)
                                            .toArray();
            long[] follower = follower_output.stream()
                                            .mapToLong(Long::longValue)
                                            .toArray(); 

            watch_resultSet.next();
            Array watch_array = watch_resultSet.getArray(1);
            String[]  watched = (watch_array == null) ? new String[0] :(String[]) watch_array.getArray();

            like_resultSet.next();
            Array like_array = like_resultSet.getArray(1);
            String[] liked = (like_array == null) ? new String[0] :(String[]) like_array.getArray();


            collect_resultSet.next();
            Array collect_array = collect_resultSet.getArray(1);
            String[] collected = (collect_array == null) ? new String[0] :(String[]) collect_array.getArray();
            
            post_resultSet.next();
            Array post_array = post_resultSet.getArray(1);
            String[] posted = (post_array == null) ? new String[0] :(String[]) post_array.getArray();

            userInfoResp = UserInfoResp.builder()
                        .mid(mid)
                        .coin(coin_output)
                        .following(following)
                        .follower(follower)
                        .watched(watched)
                        .liked(liked)
                        .collected(collected)
                        .posted(posted)
                        .build();

            coin_stmt.close();
            follow_stmt.close();
            watch_stmt.close();
            like_stmt.close();
            collect_stmt.close();
            post_stmt.close();
            //log.info("User info of "+ mid +" retrieved.");
            return userInfoResp;
        } catch (SQLException e) {
            log.error(e.getMessage());
        } 
        return null;
    };
}
