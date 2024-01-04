package io.sustc.service.impl;

import java.sql.Timestamp;
import java.sql.Array;
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
        long danmu_id = 0;
        if(content == "" || content == null){
            return -1;
        }
        String sql = "SELECT send_danmu(?, ?, ?, ?, ?, ?::NUMERIC, ?)";
        try (Connection conn = dataSource.getConnection();
            PreparedStatement stmt = conn.prepareStatement(sql)){
            stmt.setLong(1, auth.getMid());
            stmt.setString(2, auth.getPassword());
            stmt.setString(3, auth.getQq());
            stmt.setString(4, auth.getWechat());
            stmt.setString(5, bv);
            stmt.setFloat(6, time);
            stmt.setString(7, content);
            ResultSet resultSet = stmt.executeQuery();
            if(resultSet.next()){
                danmu_id = resultSet.getLong(1);
                //System.out.println("We have got the danmu_id: " + danmu_id);
            }
            stmt.close();
            return danmu_id;
        }catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        return -1;
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
        String sql = (filter) ? "SELECT display_danmu_distinct(?, ?::NUMERIC, ?::NUMERIC)" : "SELECT display_danmu_with_repetition(?, ?::NUMERIC, ?::NUMERIC)";
        List<Long> result = List.of();
        try (Connection conn = dataSource.getConnection();
            PreparedStatement stmt = conn.prepareStatement(sql)){
            stmt.setString(1, bv);
            stmt.setFloat(2, timeStart);
            stmt.setFloat(3, timeEnd);
            ResultSet resultset = stmt.executeQuery();
            if(resultset.next()){
                Array array = resultset.getArray(1);
                Long[] result_set = (Long[]) array.getArray();
                result = List.of(result_set);
            }
            stmt.close();
            return result;
        }catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        return null;
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
        //log.info("likeDanmu: " + auth.toString() + " " + id);
        //Check whether the auth info is valid. 
        String sql = "SELECT like_danmu(?, ?, ?, ?, ?)";
        try(Connection conn = dataSource.getConnection();
            PreparedStatement stmt = conn.prepareStatement(sql)){
            stmt.setLong(1, auth.getMid());
            stmt.setString(2, auth.getPassword());
            stmt.setString(3, auth.getQq());
            stmt.setString(4, auth.getWechat());
            stmt.setLong(5, id);
            ResultSet resultSet = stmt.executeQuery();
            long result = -1;
            while(resultSet.next()){
                result = resultSet.getLong(1);
            }
            stmt.close();
            if(result == -1){
                return false;
            }
            return true;
        }catch(SQLException e){
            log.error(e.getMessage());
        }
        return false;
    }   
}
