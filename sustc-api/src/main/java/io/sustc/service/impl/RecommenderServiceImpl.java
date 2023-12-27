package io.sustc.service.impl;

import java.util.List;
import java.util.Set;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import io.sustc.dto.AuthInfo;
import io.sustc.service.RecommenderService;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class RecommenderServiceImpl implements RecommenderService {
    @Autowired
    private DataSource dataSource;

    /**
     * Recommends a list of top 5 similar videos for a video.
     * The similarity is defined as the number of users (in the database) who have watched both videos.
     *
     * @param bv the current video
     * @return a list of video {@code bv}s
     * @apiNote You may consider the following corner cases:
     * <ul>
     *   <li>cannot find a video corresponding to the {@code bv}</li>
     * </ul>
     * If any of the corner case happened, {@code null} shall be returned.
     */
    @Override
    public List<String> recommendNextVideo(String bv) {
        String sql = "SELECT recommend_next_videos(?)";
        List<String> result = List.of();
        try(Connection conn = dataSource.getConnection();
            PreparedStatement stmt = conn.prepareStatement(sql)){
            stmt.setString(1, bv);
            ResultSet resultset = stmt.executeQuery();
            if(resultset.next()){
                Array array = resultset.getArray(1);
                String[] result_set = (String[]) array.getArray();
                result = List.of(result_set);
            }
            stmt.close();
            return result;
        } catch (SQLException e) {
            log.error("Connection Failed! Check output console");
        }
        return null;
    }

    /**
     * Recommends videos for anonymous users, based on the popularity.
     * Evaluate the video's popularity from the following aspects:
     * <ol>
     *   <li>"like": the rate of watched users who also liked this video</li>
     *   <li>"coin": the rate of watched users who also donated coin to this video</li>
     *   <li>"fav": the rate of watched users who also collected this video</li>
     *   <li>"danmu": the average number of danmus sent by one watched user</li>
     *   <li>"finish": the average video watched percentage of one watched user</li>
     * </ol>
     * The recommendation score can be calculated as:
     * <pre>
     *   score = like + coin + fav + danmu + finish
     * </pre>
     *
     * @param pageSize the page size, if there are less than {@code pageSize} videos, return all of them
     * @param pageNum  the page number, starts from 1
     * @return a list of video {@code bv}s, sorted by the recommendation score
     * @implNote 
     * Though users can like/coin/favorite a video without watching it, the rates of these values should be clamped to 1.
     * If no one has watched this video, all the five scores shall be 0.
     * If the requested page is empty, return an empty list.
     * @apiNote You may consider the following corner cases:
     * <ul>
     *   <li>{@code pageSize} and/or {@code pageNum} is invalid (any of them <= 0)</li>
     * </ul>
     * If any of the corner case happened, {@code null} shall be returned.
     */
    @Override
    public List<String> generalRecommendations(int pageSize, int pageNum) {
        if(pageSize <= 0 || pageNum <= 0){
            return null;
        }
        String sql = "SELECT general_recommendation(?, ?)";
        List<String> result = List.of();
        try(Connection conn = dataSource.getConnection();
            PreparedStatement stmt = conn.prepareStatement(sql)){
            stmt.setInt(1, pageSize);
            stmt.setInt(2, pageNum);
            ResultSet resultset = stmt.executeQuery();
            if(resultset.next()){
                Array array = resultset.getArray(1);
                String[] result_set = (String[]) array.getArray();
                result = List.of(result_set);
            }
            stmt.close();
            return result;
        } catch (SQLException e) {
            System.out.println("Connection Failed! Check output console");
        }
        return null;
    }

    /**
     * Recommends videos for a user, restricted on their interests.
     * The user's interests are defined as the videos that the user's friend(s) have watched,
     * filter out the videos that the user has already watched.
     * Friend(s) of current user is/are the one(s) who is/are both the current user' follower and followee at the same time.
     * Sort the videos by:
     * <ol>
     *   <li>The number of friends who have watched the video</li>
     *   <li>The video owner's level</li>
     *   <li>The video's public time (newer videos are preferred)</li>
     * </ol>
     *
     * @param auth     the current user's authentication information to be recommended
     * @param pageSize the page size, if there are less than {@code pageSize} videos, return all of them
     * @param pageNum  the page number, starts from 1
     * @return a list of video {@code bv}s
     * @implNote
     * If the current user's interest is empty, return {@link io.sustc.service.RecommenderService#generalRecommendations(int, int)}.
     * If the requested page is empty, return an empty list
     * @apiNote You may consider the following corner cases:
     * <ul>
     *   <li>{@code auth} is invalid, as stated in {@link io.sustc.service.UserService#deleteAccount(AuthInfo, long)}</li>
     *   <li>{@code pageSize} and/or {@code pageNum} is invalid (any of them <= 0)</li>
     * </ul>
     * If any of the corner case happened, {@code null} shall be returned.
     */
    @Override
    public List<String> recommendVideosForUser(AuthInfo auth, int pageSize, int pageNum) {
        if(pageNum <= 0 || pageSize <= 0){
            return null;
        }
        String sql = "SELECT recommend_videos_for_user(?, ?, ?, ?, ?, ?)";
        List<String> result = List.of();
        try(Connection conn = dataSource.getConnection();
            PreparedStatement stmt = conn.prepareStatement(sql)){
            stmt.setLong(1, auth.getMid());
            stmt.setString(2, auth.getPassword());
            stmt.setString(3, auth.getQq());
            stmt.setString(4, auth.getWechat());
            stmt.setInt(5, pageSize);
            stmt.setInt(6, pageNum);
            ResultSet resultset = stmt.executeQuery();
            if(resultset.next()){
                Array array = resultset.getArray(1);
                String[] result_set = (String[]) array.getArray();
                result = List.of(result_set);
            }
            stmt.close();
            return result;
        } catch (SQLException e) {
            System.out.println("Connection Failed! Check output console");
        }
        return null;
    }

    /**
     * Recommends friends for a user, based on their common followings.
     * Find all users that are not currently followed by the user, and have at least one common following with the user.
     * Sort the users by the number of common followings in descending order.
     * If two users have the same number of common followings,
     * sort them by their {@code level} in descending order, then by their {@code mid} in ascending order.
     *
     * @param auth     the current user's authentication information to be recommended
     * @param pageSize the page size, if there are less than {@code pageSize} users, return all of them
     * @param pageNum  the page number, starts from 1
     * @return a list of {@code mid}s of the recommended users
     * @implNote If the requested page is empty, return an empty list
     * @apiNote You may consider the following corner cases:
     * <ul>
     *   <li>{@code auth} is invalid, as stated in {@link io.sustc.service.UserService#deleteAccount(AuthInfo, long)}</li>
     *   <li>{@code pageSize} and/or {@code pageNum} is invalid (any of them <= 0)</li>
     * </ul>
     * If any of the corner case happened, {@code null} shall be returned.
     */
    @Override
    public List<Long> recommendFriends(AuthInfo auth, int pageSize, int pageNum) {
        if(pageSize <= 0 || pageNum <= 0){
            return null;
        }
        String sql = "SELECT recommend_friends_for_user(?, ?, ?, ?, ?, ?)";
        List<Long> result = List.of();
        try(Connection conn = dataSource.getConnection();
            PreparedStatement stmt = conn.prepareStatement(sql)){
            stmt.setLong(1, auth.getMid());
            stmt.setString(2, auth.getPassword());
            stmt.setString(3, auth.getQq());
            stmt.setString(4, auth.getWechat());
            stmt.setInt(5, pageSize);
            stmt.setInt(6, pageNum);
            ResultSet resultset = stmt.executeQuery();
            if(resultset.next()){
                Array array = resultset.getArray(1);
                Long[] result_set = (Long[]) array.getArray();
                result = List.of(result_set);
            }
            stmt.close();
            return result;
        } catch (SQLException e) {
            System.out.println("Connection Failed! Check output console");
        }
        return null;
    }
    
}
