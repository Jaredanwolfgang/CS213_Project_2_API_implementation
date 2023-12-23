package io.sustc.service.impl;

import java.sql.Timestamp;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import javax.naming.spi.DirStateFactory.Result;
import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cglib.core.Local;
import org.springframework.stereotype.Service;

import io.sustc.dto.AuthInfo;
import io.sustc.service.DanmuService;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class DanmuServiceImpl implements DanmuService{
    @Autowired
    private DataSource dataSource;
    /**
     * Sends a danmu to a video.
     * It is mandatory that the user shall watch the video first before he/she can send danmu to it.
     *
     * @param auth    the current user's authentication information
     * @param bv      the video's bv
     * @param content the content of danmu
     * @param time    seconds since the video starts
     * @return the generated danmu id
     * @apiNote You may consider the following corner cases:
     * <ul>
     *   <li>{@code auth} is invalid, as stated in {@link io.sustc.service.UserService#deleteAccount(AuthInfo, long)}</li>
     *   <li>cannot find a video corresponding to the {@code bv}</li>
     *   <li>{@code content} is invalid (null or empty)</li>
     *   <li>the video is not published or the user has not watched this video</li>
     * </ul>
     * If any of the corner case happened, {@code -1} shall be returned.
     */
    @Override
    public long sendDanmu(AuthInfo auth, String bv, String content, float time){
        long mid = 0;
        if(content == "" || content == null){
            return -1;
        }
        try (Connection conn = dataSource.getConnection()){
            //Check whether the bv is valid.
            String validation_bv = "SELECT is_bv_valid (?)";
            try(PreparedStatement stmt = conn.prepareStatement(validation_bv)){
                stmt.setString(1, bv);
                ResultSet resultSet = stmt.executeQuery();
                while(resultSet.next()){
                    if(resultSet.getInt(1) == 0){
                        return -1;
                    }
                }
                stmt.close();
                System.out.println("validation for video succeeded");
            }catch(SQLException e){
                System.out.println("validation for video failed");
            }
            
            //Check whether the video is public.
            String validation_public = "SELECT video_public (?)";
            try(PreparedStatement stmt = conn.prepareStatement(validation_public)){
                stmt.setString(1, bv);
                ResultSet resultSet = stmt.executeQuery();
                while(resultSet.next()){
                    if(resultSet.getInt(1) == 0){
                        return -1;
                    }
                }
                stmt.close();
                System.out.println("validation for public video succeeded");
            }catch (SQLException e){
                System.out.println("validation for public video failed");
            }

            String validation_auth = "SELECT is_auth_valid(?, ?, ?, ?)";
            try(PreparedStatement stmt = conn.prepareStatement(validation_auth)){
                stmt.setLong(1, auth.getMid());
                stmt.setString(2, auth.getPassword());
                stmt.setString(3, auth.getQq());
                stmt.setString(4, auth.getWechat());
                ResultSet resultSet = stmt.executeQuery();
                while(resultSet.next()){
                    if(resultSet.getLong(1) == 0){
                        return -1;
                    }else{
                        mid = resultSet.getLong(1);
                    }
                }
                stmt.close();
                System.out.println("SendDanmu validation succeeded.");
            }catch (SQLException e){
                System.out.println("SendDanmu validation failed.");
            }

            //After validation insert the data. 
            String insert = "INSERT INTO danmu (bv, mid, content, displaytime, posttime) values";
            insert += "(?, ?, ?, ?, ?)";
            try(PreparedStatement stmt = conn.prepareStatement(insert)){
                stmt.setString(1, bv);
                stmt.setLong(2, mid);
                stmt.setString(3, content);
                stmt.setFloat(4, time);
                LocalDateTime currentDateTime = LocalDateTime.now();
                Timestamp timestamp = Timestamp.valueOf(currentDateTime);
                stmt.setTimestamp(5, timestamp);
                stmt.execute();
                stmt.close();
            } catch (SQLException e) {
                System.out.println("Danmu not sent!");
            }
        }catch (SQLException e) {
            System.out.println("Connection failed");
        }
        return 0;
    }

    /**
     * Display the danmus in a time range.
     * Similar to bilibili's mechanism, user can choose to only display part of the danmus to have a better watching
     * experience.
     *
     * @param bv        the video's bv
     * @param timeStart the start time of the range
     * @param timeEnd   the end time of the range
     * @param filter    whether to remove the duplicated content,
     *                  if {@code true}, only the earliest posted danmu with the same content shall be returned
     * @return a list of danmus id, sorted by {@code time}
     * @apiNote You may consider the following corner cases:
     * <ul>
     *   <li>cannot find a video corresponding to the {@code bv}</li>
     *   <li>
     *     {@code timeStart} and/or {@code timeEnd} is invalid ({@code timeStart} <= {@code timeEnd}
     *     or any of them < 0 or > video duration)
     *   </li>
     *   <li>the video is not published</li>
     * </ul>
     * If any of the corner case happened, {@code null} shall be returned.
     */
    @Override
    public List<Long> displayDanmu(String bv, float timeStart, float timeEnd, boolean filter){
        List<Long> result_danmus = new ArrayList<>();
        
        try (Connection conn = dataSource.getConnection()){
            //Check whether the bv is valid.
            String validation_bv = "SELECT is_bv_valid (?)";
            try(PreparedStatement stmt = conn.prepareStatement(validation_bv)){
                stmt.setString(1, bv);
                ResultSet resultSet = stmt.executeQuery();
                while(resultSet.next()){
                    if(resultSet.getInt(1) == 0){
                        return null;
                    }
                }
                stmt.close();
                System.out.println("validation for video succeeded");
            }catch(SQLException e){
                System.out.println("validation for video failed");
            }

            //Check whether the video is public.
            String validation_public = "SELECT video_public (?)";
            try(PreparedStatement stmt = conn.prepareStatement(validation_public)){
                stmt.setString(1, bv);
                ResultSet resultSet = stmt.executeQuery();
                while(resultSet.next()){
                    if(resultSet.getInt(1) == 0){
                        return null;
                    }
                }
                stmt.close();
                System.out.println("validation for public video succeeded");
            }catch (SQLException e){
                System.out.println("validation for public video failed");
            }

            //Check whether the time is valid.
            String validation_time = "SELECT time_valid (?, ?, ?)";
            try(PreparedStatement stmt = conn.prepareStatement(validation_time)){
                stmt.setString(1, bv);
                stmt.setFloat(2, timeStart);
                stmt.setFloat(3, timeEnd);
                ResultSet resultSet = stmt.executeQuery();
                while(resultSet.next()){
                    if(resultSet.getInt(1) == 0){
                        return null;
                    }
                }
                stmt.close();
                System.out.println("validation for time succeeded");
            }catch (SQLException e){
                System.out.println("validation for time failed");
            }

            //Check what quest to make. 
            String quest;
            if(filter){
                quest = "SELECT display_Danmu_distinct(?, ?, ?)";
            }else{
                quest = "SELECT display_Danmu_with_Repetition(?, ?, ?)";
            }
            try(PreparedStatement stmt = conn.prepareStatement(quest)){
                stmt.setString(1, bv);
                stmt.setFloat(2, timeStart);
                stmt.setFloat(3, timeEnd);
                ResultSet resultSet = stmt.executeQuery();
                while(resultSet.next()){
                    result_danmus.add(Long.valueOf(resultSet.getLong(1)));
                }
            }catch (SQLException e){
                System.out.println("Display Danmu failed.");
            }
            System.out.println("Display Danmu succeeded.");
        }catch (SQLException e) {
            System.out.println("Connection failed");
        }
        if(result_danmus != null){
            return result_danmus;
        }else{
            return null;
        }
    }

    /**
     * Likes a danmu.
     * If the user already liked the danmu, this operation will cancel the like status.
     * It is mandatory that the user shall watch the video first before he/she can like a danmu of it.
     *
     * @param auth the current user's authentication information
     * @param id   the danmu's id
     * @return the like state of the user to this danmu after this operation
     * @apiNote You may consider the following corner cases:
     * <ul>
     *   <li>{@code auth} is invalid, as stated in {@link io.sustc.service.UserService#deleteAccount(AuthInfo, long)}</li>
     *   <li>cannot find a danmu corresponding to the {@code id}</li>
     * </ul>
     * If any of the corner case happened, {@code false} shall be returned.
     */
    @Override
    public boolean likeDanmu(AuthInfo auth, long id){
        try(Connection conn = dataSource.getConnection()){
            //Check whether the auth info is valid. 
            long mid = 0;
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
                    }else{
                        mid = resultSet.getLong(1);
                    }
                }
                stmt.close();
                System.out.println("Auth validation succeeded.");
            }catch (SQLException e){
                System.out.println("Auth validation failed.");
            }

            //Check whether the danmu is valid.
            String validation_id = "SELECT is_danmu_valid (?)";
            try(PreparedStatement stmt = conn.prepareStatement(validation_id)){
                stmt.setLong(1, id);
                ResultSet resultSet = stmt.executeQuery();
                while(resultSet.next()){
                    if(resultSet.getInt(1) == 0){
                        return false;
                    }
                }
                stmt.close();
                System.out.println("validation for danmu succeeded");
            }catch(SQLException e){
                System.out.println("validation for danmu failed");
            }

            //Insert into Like Danmu
            String insert = "INSERT INTO like_danmu (danmu_id, mid) values";
            insert += "(?, ?)";
            try(PreparedStatement stmt = conn.prepareStatement(insert)){
                stmt.setLong(1, id);
                stmt.setLong(2, mid);
                stmt.execute();
                stmt.close();
                System.out.println("Like danmu inserted");
            } catch (SQLException e) {
                System.out.println("Likde danmu not inserted");
            }
        }catch(SQLException e){
            System.out.println("Connection failed.");
        }
        return true;
    }   
}
