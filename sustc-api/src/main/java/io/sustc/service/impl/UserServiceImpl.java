package io.sustc.service.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

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
        if(req.getBirthday()!=null&&!req.getBirthday().matches("\\d{1,2}月\\d{1,2}日")){
            return -1;
        }
        try(Connection conn = dataSource.getConnection()){
            String validation = "select is_reg_valid(?,?,?)";
            try(PreparedStatement stmt = conn.prepareStatement(validation)){
                stmt.setString(1, req.getName());
                stmt.setString(2, req.getQq());
                stmt.setString(3, req.getWechat());
                ResultSet resultSet = stmt.executeQuery();
                while(resultSet.next()){
                    if(resultSet.getInt(1) == 0){
                        return -1;
                    }
                }
                stmt.close();
                System.out.println("validation for user register succeeded");
            }catch(SQLException e){
                System.out.println("validation for user register failed");
            }
            String generate_mid = "select generate_mid()";
            long mid = -1;
            try(PreparedStatement stmt = conn.prepareStatement(generate_mid)){
                ResultSet resultSet = stmt.executeQuery();
                while(resultSet.next()){
                    mid = resultSet.getLong(1);
                    System.out.println("mid generated: "+mid);
                }
                stmt.close();
                System.out.println("mid generation succeeded");
            }catch(SQLException e){
                System.out.println("mid generation failed");
            }
            String insert_user = "insert into user values(?,?,?,?,?,?,?,?)";
            try(PreparedStatement stmt2 = conn.prepareStatement(insert_user)){
                stmt2.setLong(1, mid);
                stmt2.setString(2, req.getName());
                stmt2.setString(3, req.getSex() == Gender.MALE ? "男" : (req.getSex() == Gender.FEMALE ? "女" : "保密"));
                stmt2.setString(4, req.getBirthday());
                stmt2.setShort(5, (short)1);
                stmt2.setInt(6, 0);
                stmt2.setString(7, req.getSign());
                stmt2.setString(8, req.getPassword());
                stmt2.executeUpdate();
                System.out.println("user inserted");
                return mid;
            }catch(SQLException e){
                System.out.println("user insert failed");
            }
        } catch (SQLException e) {
            System.out.println("Connection failed");
        }
        return -1;
    };

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
        try (Connection conn = dataSource.getConnection()) {
            String validation_auth = "SELECT is_auth_valid(?, ?, ?, ?)";
            try(PreparedStatement stmt = conn.prepareStatement(validation_auth)){
                stmt.setLong(1, auth.getMid());
                stmt.setString(2, auth.getPassword());
                stmt.setString(3, auth.getQq());
                stmt.setString(4, auth.getWechat());
                ResultSet resultSet = stmt.executeQuery();
                while(resultSet.next()){
                    if(resultSet.getLong(1) == 0){
                        return false;
                    }
                }
                stmt.close();
                System.out.println("Auth validation succeeded.");
            }catch (SQLException e){
                System.out.println("Auth validation failed.");
            }
            String delete = "SELECT delete_user(?, ?)";
            try(PreparedStatement stmt = conn.prepareStatement(delete)){
                stmt.setLong(1, auth.getMid());
                stmt.setLong(2, mid);
                ResultSet resultSet = stmt.executeQuery();
                while(resultSet.next()){
                    if(resultSet.getLong(1) == 0){
                        return false;
                    }
                }
                stmt.close();
                System.out.println("User deleted.");
                return true;
            }catch (SQLException e){
                System.out.println("User deletion failed.");
            }
        } catch (Exception e) {
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
            String validation_auth = "SELECT is_auth_valid(?, ?, ?, ?)";
            try(PreparedStatement stmt = conn.prepareStatement(validation_auth)){
                stmt.setLong(1, auth.getMid());
                stmt.setString(2, auth.getPassword());
                stmt.setString(3, auth.getQq());
                stmt.setString(4, auth.getWechat());
                ResultSet resultSet = stmt.executeQuery();
                while(resultSet.next()){
                    if(resultSet.getLong(1) == 0){
                        return false;
                    }
                }
                stmt.close();
                System.out.println("Auth validation succeeded.");
            }catch (SQLException e){
                System.out.println("Auth validation failed.");
            }
            String follow = "SELECT follow_user(?, ?)";
            try(PreparedStatement stmt = conn.prepareStatement(follow)){
                stmt.setLong(1, auth.getMid());
                stmt.setLong(2, followeeMid);
                ResultSet resultSet = stmt.executeQuery();
                while(resultSet.next()){
                    if(resultSet.getLong(1) == 0){
                        return false;
                    }
                }
                stmt.close();
                System.out.println("Followed");
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
        try (Connection conn = dataSource.getConnection()) {
            String query = "SELECT * FROM user WHERE mid = ?";
            try(PreparedStatement stmt = conn.prepareStatement(query)){
                stmt.setLong(1, mid);
                ResultSet resultSet = stmt.executeQuery();
                while(resultSet.next()){
                    userInfoResp = UserInfoResp.builder()
                            .mid(resultSet.getLong(1))
                            .coin(resultSet.getInt(6))
                            .build();
                }
                stmt.close();
                System.out.println("User info mid coin retrieved.");
            }catch (SQLException e){
                System.out.println("User info mid coin retrieval failed.");
            }
            String followee = "SELECT followee_mid FROM follow WHERE follower_mid = ?";
            try(PreparedStatement stmt = conn.prepareStatement(followee)){
                stmt.setLong(1, mid);
                ResultSet resultSet = stmt.executeQuery();
                long [] following = new long[resultSet.getFetchSize()];
                int count = 0;
                while(resultSet.next()){
                    following[count] = resultSet.getLong(1);
                    count++;
                }
                userInfoResp = UserInfoResp.builder()
                            .following(following)
                            .build();
                stmt.close();
                System.out.println("User info following retrieved.");
            }catch (SQLException e){
                System.out.println("User info following retrieval failed.");
            }
            String follower = "SELECT follower_mid FROM follow WHERE followee_mid = ?";
            try(PreparedStatement stmt = conn.prepareStatement(follower)){
                stmt.setLong(1, mid);
                ResultSet resultSet = stmt.executeQuery();
                long [] follower_mid = new long[resultSet.getFetchSize()];
                int count = 0;
                while(resultSet.next()){
                    follower_mid[count] = resultSet.getLong(1);
                    count++;
                }
                userInfoResp = UserInfoResp.builder()
                            .follower(follower_mid)
                            .build();
                stmt.close();
                System.out.println("User info follower retrieved.");
            }catch (SQLException e){
                System.out.println("User info follower retrieval failed.");
            }
            String watch = "SELECT bv FROM watch WHERE mid = ?";
            try(PreparedStatement stmt = conn.prepareStatement(watch)){
                stmt.setLong(1, mid);
                ResultSet resultSet = stmt.executeQuery();
                String [] watched = new String[resultSet.getFetchSize()];
                int count = 0;
                while(resultSet.next()){
                    watched[count] = resultSet.getString(1);
                    count++;
                }
                userInfoResp = UserInfoResp.builder()
                            .watched(watched)
                            .build();
                stmt.close();
                System.out.println("User info watch retrieved.");
            }catch (SQLException e){
                System.out.println("User info watch retrieval failed.");
            }
            String like = "SELECT bv FROM like_video WHERE mid = ?";
            try(PreparedStatement stmt = conn.prepareStatement(like)){
                stmt.setLong(1, mid);
                ResultSet resultSet = stmt.executeQuery();
                String [] liked = new String[resultSet.getFetchSize()];
                int count = 0;
                while(resultSet.next()){
                    liked[count] = resultSet.getString(1);
                    count++;
                }
                userInfoResp = UserInfoResp.builder()
                            .liked(liked)
                            .build();
                stmt.close();
                System.out.println("User info like retrieved.");
            }catch (SQLException e){
                System.out.println("User info like retrieval failed.");
            }
            String collect = "SELECT bv FROM favorite WHERE mid = ?";
            try(PreparedStatement stmt = conn.prepareStatement(collect)){
                stmt.setLong(1, mid);
                ResultSet resultSet = stmt.executeQuery();
                String [] collected = new String[resultSet.getFetchSize()];
                int count = 0;
                while(resultSet.next()){
                    collected[count] = resultSet.getString(1);
                    count++;
                }
                userInfoResp = UserInfoResp.builder()
                            .collected(collected)
                            .build();
                stmt.close();
                System.out.println("User info collect retrieved.");
            }catch (SQLException e){
                System.out.println("User info collect retrieval failed.");
            }
            String post = "SELECT bv FROM video WHERE owner_mid = ?";
            try(PreparedStatement stmt = conn.prepareStatement(post)){
                stmt.setLong(1, mid);
                ResultSet resultSet = stmt.executeQuery();
                String [] posted = new String[resultSet.getFetchSize()];
                int count = 0;
                while(resultSet.next()){
                    posted[count] = resultSet.getString(1);
                    count++;
                }
                userInfoResp = UserInfoResp.builder()
                            .posted(posted)
                            .build();
                stmt.close();
                System.out.println("User info post retrieved.");
            }catch (SQLException e){
                System.out.println("User info post retrieval failed.");
            }
            return userInfoResp;
        } catch (Exception e) {
            System.out.println("Connection failed.");
            
        } 
        return null;
    };
}
