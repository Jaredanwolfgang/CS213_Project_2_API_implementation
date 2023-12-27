package io.sustc.service.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Array;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.PostVideoReq;
import io.sustc.service.VideoService;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class VideoServiceImpl implements VideoService {
    @Autowired
    private DataSource dataSource;

    @Override
    /**
     * Posts a video. Its commit time shall be {@link LocalDateTime#now()}.
     *
     * @param auth the current user's authentication information
     * @param req  the video's information
     * @return the video's {@code bv}
     * @apiNote You may consider the following corner cases:
     * <ul>
     *   <li>{@code auth} is invalid, as stated in {@link io.sustc.service.UserService#deleteAccount(AuthInfo, long)}</li>
     *   <li>{@code req} is invalid
     *     <ul>
     *       <li>{@code title} is null or empty</li>
     *       <li>there is another video with same {@code title} and same user</li>
     *       <li>{@code duration} is less than 10 (so that no chunk can be divided)</li>
     *       <li>{@code publicTime} is earlier than {@link LocalDateTime#now()}</li>
     *     </ul>
     *   </li>
     * </ul>
     * If any of the corner case happened, {@code null} shall be returned.
     */
    public String postVideo(AuthInfo auth, PostVideoReq req) {
        String postVideo = "SELECT create_video(?, ?, ?, ?, ?, ?, ?, ?)";
        try(Connection conn = dataSource.getConnection();
            PreparedStatement stmt = conn.prepareStatement(postVideo)) {
            stmt.setLong(1, auth.getMid());
            stmt.setString(2,auth.getPassword());
            stmt.setString(3, auth.getQq());
            stmt.setString(4, auth.getWechat());
            stmt.setString(5, req.getTitle());
            stmt.setString(6, req.getDescription());
            stmt.setFloat(7, req.getDuration());
            stmt.setTimestamp(8, req.getPublicTime());
            ResultSet resultset = stmt.executeQuery();
            if(resultset.next()){
                return resultset.getString(1);
            }
            return null;
        } catch (SQLException e) {
            log.error("Failed to post video", e);
            return null;
        }
    }

    @Override
    /**
     * Deletes a video.
     * This operation can be performed by the video owner or a superuser.
     *
     * @param auth the current user's authentication information
     * @param bv   the video's {@code bv}
     * @return success or not
     * @apiNote You may consider the following corner cases:
     * <ul>
     *   <li>{@code auth} is invalid, as stated in {@link io.sustc.service.UserService#deleteAccount(AuthInfo, long)}</li>
     *   <li>cannot find a video corresponding to the {@code bv}</li>
     *   <li>{@code auth} is not the owner of the video nor a superuser</li>
     * </ul>
     * If any of the corner case happened, {@code false} shall be returned.
     */
    public boolean deleteVideo(AuthInfo auth, String bv) {
        String deleteVideo = "SELECT delete_video(?, ?, ?, ?, ?)";
        try(Connection conn = dataSource.getConnection();
            PreparedStatement stmt = conn.prepareStatement(deleteVideo)) {
            stmt.setLong(1, auth.getMid());
            stmt.setString(2,auth.getPassword());
            stmt.setString(3, auth.getQq());
            stmt.setString(4, auth.getWechat());
            stmt.setString(5, bv);
            ResultSet resultset = stmt.executeQuery();
            if(resultset.next()){
                return resultset.getBoolean(1);
            }
            return false;
        } catch (SQLException e) {
            log.error("Failed to delete video", e);
            return false;
        }
    }

    @Override
    /**
     * Updates the video's information.
     * Only the owner of the video can update the video's information.
     * If the video was reviewed before, a new review for the updated video is required.
     * The duration shall not be modified and therefore the likes, favorites and danmus are not required to update.
     *
     * @param auth the current user's authentication information
     * @param bv   the video's {@code bv}
     * @param req  the new video information
     * @return {@code true} if the video needs to be re-reviewed (was reviewed before), {@code false} otherwise
     * @apiNote You may consider the following corner cases:
     * <ul>
     *   <li>{@code auth} is invalid, as stated in {@link io.sustc.service.UserService#deleteAccount(AuthInfo, long)}</li>
     *   <li>cannot find a video corresponding to the {@code bv}</li>
     *   <li>{@code auth} is not the owner of the video</li>
     *   <li>{@code req} is invalid, as stated in {@link io.sustc.service.VideoService#postVideo(AuthInfo, PostVideoReq)}</li>
     *   <li>{@code duration} in {@code req} is changed compared to current one</li>
     *   <li>{@code req} is not changed compared to current information</li>
     * </ul>
     * If any of the corner case happened, {@code false} shall be returned.
     */
    public boolean updateVideoInfo(AuthInfo auth, String bv, PostVideoReq req) {
        String updateVideo = "SELECT update_video(?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try(Connection conn = dataSource.getConnection();
            PreparedStatement stmt = conn.prepareStatement(updateVideo)) {
            stmt.setLong(1, auth.getMid());
            stmt.setString(2,auth.getPassword());
            stmt.setString(3, auth.getQq());
            stmt.setString(4, auth.getWechat());
            stmt.setString(5, bv);
            stmt.setString(6, req.getTitle());
            stmt.setString(7, req.getDescription());
            stmt.setFloat(8, req.getDuration());
            stmt.setTimestamp(9, req.getPublicTime());
            ResultSet resultset = stmt.executeQuery();
            if(resultset.next()){
                return resultset.getBoolean(1);
            }
            return false;
        } catch (SQLException e) {
            log.error("Failed to update video", e);
            return false;
        }
    }

    @Override
    /**
     * Search the videos by keywords (split by space).
     * You should try to match the keywords case-insensitively in the following fields:
     * <ol>
     *   <li>title</li>
     *   <li>description</li>
     *   <li>owner name</li>
     * </ol>
     * <p>
     * Sort the results by the relevance (sum up the number of keywords matched in the three fields).
     * <ul>
     *   <li>If a keyword occurs multiple times, it should be counted more than once.</li>
     *   <li>
     *     A character in these fields can only be counted once for each keyword
     *     but can be counted for different keywords.
     *   </li>
     *   <li>If two videos have the same relevance, sort them by the number of views.</li>
     * </ul>
     * <p>
     * Examples:
     * <ol>
     *   <li>
     *     If the title is "1122" and the keywords are "11 12",
     *     then the relevance in the title is 2 (one for "11" and one for "12").
     *   </li>
     *   <li>
     *     If the title is "111" and the keyword is "11",
     *     then the relevance in the title is 1 (one for the occurrence of "11").
     *   </li>
     *   <li>
     *     Consider a video with title "Java Tutorial", description "Basic to Advanced Java", owner name "John Doe".
     *     If the search keywords are "Java Advanced",
     *     then the relevance is 3 (one occurrence in the title and two in the description).
     *   </li>
     * </ol>
     * <p>
     * Unreviewed or unpublished videos are only visible to superusers or the video owner.
     *
     * @param auth     the current user's authentication information
     * @param keywords the keywords to search, e.g. "sustech database final review"
     * @param pageSize the page size, if there are less than {@code pageSize} videos, return all of them
     * @param pageNum  the page number, starts from 1
     * @return a list of video {@code bv}s
     * @implNote If the requested page is empty, return an empty list
     * @apiNote You may consider the following corner cases:
     * <ul>
     *   <li>{@code auth} is invalid, as stated in {@link io.sustc.service.UserService#deleteAccount(AuthInfo, long)}</li>
     *   <li>{@code keywords} is null or empty</li>
     *   <li>{@code pageSize} and/or {@code pageNum} is invalid (any of them <= 0)</li>
     * </ul>
     * If any of the corner case happened, {@code null} shall be returned.
     */
    public List<String> searchVideo(AuthInfo auth, String keywords, int pageSize, int pageNum) {
        if(keywords == null || keywords.isEmpty() || pageSize <= 0 || pageNum <= 0){
            return null;
        }
        String[] keyword = keywords.split(" ");
        String searchVideoSql = "SELECT search_video(?, ?, ?, ?, ?, ?, ?)";
        List<String> result = null;
        try(Connection conn = dataSource.getConnection();
            PreparedStatement stmt = conn.prepareStatement(searchVideoSql)) {
            Array keywordArray = conn.createArrayOf("varchar", keyword);
            stmt.setLong(1, auth.getMid());
            stmt.setString(2,auth.getPassword());
            stmt.setString(3, auth.getQq());
            stmt.setString(4, auth.getWechat());
            stmt.setArray(5, keywordArray);
            stmt.setInt(6, pageSize);
            stmt.setInt(7, pageNum);
            ResultSet resultset = stmt.executeQuery();
            if(resultset.next()){
                Array array = resultset.getArray(1);
                if(array != null){
                    String[] result_set = (String[]) array.getArray();
                    result = List.of(result_set);
                    return result;
                }else{
                    return null;
                }
            }
            return null;
        } catch (SQLException e) {
            log.error("Failed to search video", e);
            return null;
        }
    }

    @Override
    /**
     * Calculates the average view rate of a video.
     * The view rate is defined as the user's view time divided by the video's duration.
     *
     * @param bv the video's {@code bv}
     * @return the average view rate
     * @apiNote You may consider the following corner cases:
     * <ul>
     *   <li>cannot find a video corresponding to the {@code bv}</li>
     *   <li>no one has watched this video</li>
     * </ul>
     * If any of the corner case happened, {@code -1} shall be returned.
     */
    public double getAverageViewRate(String bv) {
        String getAverageViewRate = "SELECT get_average_view_rate(?)";
        try(Connection conn = dataSource.getConnection();
            PreparedStatement stmt = conn.prepareStatement(getAverageViewRate)) {
            stmt.setString(1, bv);
            ResultSet resultset = stmt.executeQuery();
            if(resultset.next()){
                return resultset.getDouble(1);
            }
            return -1;
        } catch (SQLException e) {
            log.error("Failed to get average view rate", e);
            return -1;
        }
    }

    @Override
    /**
     * Gets the hotspot of a video.
     * With splitting the video into 10-second chunks, hotspots are defined as chunks with the most danmus.
     *
     * @param bv the video's {@code bv}
     * @return the index of hotspot chunks (start from 0)
     * @apiNote You may consider the following corner cases:
     * <ul>
     *   <li>cannot find a video corresponding to the {@code bv}</li>
     *   <li>no one has sent danmu on this video</li>
     * </ul>
     * If any of the corner case happened, an empty set shall be returned.
     */
    public Set<Integer> getHotspot(String bv) {
        String getHotspot = "SELECT find_hotspot(?)";
        Set<Integer> result = Set.of();
        try(Connection conn = dataSource.getConnection();
            PreparedStatement stmt = conn.prepareStatement(getHotspot)) {
            stmt.setString(1, bv);
            ResultSet resultset = stmt.executeQuery();
            if(resultset.next()){
                Array array = resultset.getArray(1);
                Integer[] result_set = (Integer[]) array.getArray();
                result = Set.of(result_set);
                return result;
            }
            return result;
        } catch (SQLException e) {
            log.error("Failed to get hotspot", e);
            return result;
        }
    }

    @Override
    /**
     * Reviews a video by a superuser.
     * If the video is already reviewed, do not modify the review info.
     *
     * @param auth the current user's authentication information
     * @param bv   the video's {@code bv}
     * @return {@code true} if the video is newly successfully reviewed, {@code false} otherwise
     * @apiNote You may consider the following corner cases:
     * <ul>
     *   <li>{@code auth} is invalid, as stated in {@link io.sustc.service.UserService#deleteAccount(AuthInfo, long)}</li>
     *   <li>cannot find a video corresponding to the {@code bv}</li>
     *   <li>{@code auth} is not a superuser or he/she is the owner</li>
     *   <li>the video is already reviewed</li>
     * </ul>
     * If any of the corner case happened, {@code false} shall be returned.
     */
    public boolean reviewVideo(AuthInfo auth, String bv) {
        String reviewVideo = "SELECT review_video(?, ?, ?, ?, ?)";
        try(Connection conn = dataSource.getConnection();
            PreparedStatement stmt = conn.prepareStatement(reviewVideo)) {
            stmt.setLong(1, auth.getMid());
            stmt.setString(2,auth.getPassword());
            stmt.setString(3, auth.getQq());
            stmt.setString(4, auth.getWechat());
            stmt.setString(5, bv);
            ResultSet resultset = stmt.executeQuery();
            if(resultset.next()){
                return resultset.getBoolean(1);
            }
            return false;
        } catch (SQLException e) {
            log.error("Failed to review video", e);
            return false;
        }
    }

    @Override
    /**
     * Donates one coin to the video. A user can at most donate one coin to a video.
     * The user can only coin a video if he/she can search it ({@link io.sustc.service.VideoService#searchVideo(AuthInfo, String, int, int)}).
     * It is not mandatory that the user shall watch the video first before he/she donates coin to it.
     *
     * @param auth the current user's authentication information
     * @param bv   the video's {@code bv}
     * @return whether a coin is successfully donated
     * @apiNote You may consider the following corner cases:
     * <ul>
     *   <li>{@code auth} is invalid, as stated in {@link io.sustc.service.UserService#deleteAccount(AuthInfo, long)}</li>
     *   <li>cannot find a video corresponding to the {@code bv}</li>
     *   <li>the user cannot search this video or he/she is the owner</li>
     *   <li>the user has no coin or has donated a coin to this video</li>
     * </ul>
     * If any of the corner case happened, {@code false} shall be returned.
     */
    public boolean coinVideo(AuthInfo auth, String bv) {
        String coinVideo = "SELECT coin_video(?, ?, ?, ?, ?)";
        try(Connection conn = dataSource.getConnection();
            PreparedStatement stmt = conn.prepareStatement(coinVideo)) {
            stmt.setLong(1, auth.getMid());
            stmt.setString(2,auth.getPassword());
            stmt.setString(3, auth.getQq());
            stmt.setString(4, auth.getWechat());
            stmt.setString(5, bv);
            ResultSet resultset = stmt.executeQuery();
            if(resultset.next()){
                return resultset.getBoolean(1);
            }
            return false;
        } catch (SQLException e) {
            log.error("Failed to coin video", e);
            return false;
        }
    }

    @Override
    /**
     * Likes a video.
     * The user can only like a video if he/she can search it ({@link io.sustc.service.VideoService#searchVideo(AuthInfo, String, int, int)}).
     * If the user already liked the video, the operation will cancel the like.
     * It is not mandatory that the user shall watch the video first before he/she likes to it.
     *
     * @param auth the current user's authentication information
     * @param bv   the video's {@code bv}
     * @return the like state of the user to this video after this operation
     * @apiNote You may consider the following corner cases:
     * <ul>
     *   <li>{@code auth} is invalid, as stated in {@link io.sustc.service.UserService#deleteAccount(AuthInfo, long)}</li>
     *   <li>cannot find a video corresponding to the {@code bv}</li>
     *   <li>the user cannot search this video or the user is the video owner</li>
     * </ul>
     * If any of the corner case happened, {@code false} shall be returned.
     */
    public boolean likeVideo(AuthInfo auth, String bv) {
        String likeVideo = "SELECT like_video(?, ?, ?, ?, ?)";
        try(Connection conn = dataSource.getConnection();
            PreparedStatement stmt = conn.prepareStatement(likeVideo)) {
            stmt.setLong(1, auth.getMid());
            stmt.setString(2,auth.getPassword());
            stmt.setString(3, auth.getQq());
            stmt.setString(4, auth.getWechat());
            stmt.setString(5, bv);
            ResultSet resultset = stmt.executeQuery();
            if(resultset.next()){
                return resultset.getBoolean(1);
            }
            return false;
        } catch (SQLException e) {
            log.error("Failed to like video", e);
            return false;
        }
    }

    @Override
    /**
     * Collects a video.
     * The user can only collect a video if he/she can search it.
     * If the user already collected the video, the operation will cancel the collection.
     * It is not mandatory that the user shall watch the video first before he/she collects coin to it.
     *
     * @param auth the current user's authentication information
     * @param bv   the video's {@code bv}
     * @return the collect state of the user to this video after this operation
     * @apiNote You may consider the following corner cases:
     * <ul>
     *   <li>{@code auth} is invalid, as stated in {@link io.sustc.service.UserService#deleteAccount(AuthInfo, long)}</li>
     *   <li>cannot find a video corresponding to the {@code bv}</li>
     *   <li>the user cannot search this video or the user is the video owner</li>
     * </ul>
     * If any of the corner case happened, {@code false} shall be returned.
     */
    public boolean collectVideo(AuthInfo auth, String bv) {
        String collectVideo = "SELECT collect_video(?, ?, ?, ?, ?)";
        try(Connection conn = dataSource.getConnection();
            PreparedStatement stmt = conn.prepareStatement(collectVideo)) {
            stmt.setLong(1, auth.getMid());
            stmt.setString(2,auth.getPassword());
            stmt.setString(3, auth.getQq());
            stmt.setString(4, auth.getWechat());
            stmt.setString(5, bv);
            ResultSet resultset = stmt.executeQuery();
            if(resultset.next()){
                return resultset.getBoolean(1);
            }
            return false;
        } catch (SQLException e) {
            log.error("Failed to collect video", e);
            return false;
        }
    }
    
}
