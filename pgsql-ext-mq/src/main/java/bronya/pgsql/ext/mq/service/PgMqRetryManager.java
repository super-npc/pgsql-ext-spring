package bronya.pgsql.ext.mq.service;

import lombok.extern.slf4j.Slf4j;
import org.andreaesposito.pgmq.jdbc.client.Json;

import java.util.UUID;

/**
 * PgMQ 重试管理器
 * <p>
 * 功能：
 * 1. 创建带有重试信息的 headers
 * 2. 从 headers 中读取和更新重试计数
 * 3. 判断是否超过最大重试次数
 * 4. 生成重试延迟时间
 */
@Slf4j
public class PgMqRetryManager {
    
    // Headers 中的字段名
    private static final String HEADER_RETRY_COUNT = "x-retry-count";
    private static final String HEADER_MAX_RETRY = "x-max-retry";
    private static final String HEADER_RETRY_DELAY = "x-retry-delay";
    private static final String HEADER_RETRY_TRACE_ID = "x-retry-trace-id";
    
    /**
     * 创建初始化 Headers（发送消息时调用）
     */
    public static Json createInitialHeaders(int maxRetry, int retryDelaySeconds) {
        Json headers = new Json("{}");
        
        try {
            // 构建 headers JSON
            String headersJson = String.format(
                "{\"%s\":%d,\"%s\":%d,\"%s\":0,\"%s\":\"%s\"}",
                HEADER_MAX_RETRY, maxRetry,
                HEADER_RETRY_DELAY, retryDelaySeconds,
                HEADER_RETRY_COUNT,
                HEADER_RETRY_TRACE_ID, UUID.randomUUID().toString().substring(0, 8)
            );
            headers = new Json(headersJson);
            log.debug("创建 Headers: maxRetry={}, retryDelay={}s", maxRetry, retryDelaySeconds);
        } catch (Exception e) {
            log.warn("创建 Headers 失败，使用空 Headers", e);
        }
        
        return headers;
    }
    
    /**
     * 解析 Headers 获取重试次数
     */
    public static int getRetryCount(Json headers) {
        if (headers == null || headers.value() == null || "{}".equals(headers.value())) {
            return 0;
        }
        
        try {
            String value = headers.value();
            // 简单的 JSON 解析，查找 retry-count 字段
            String key = String.format("\"%s\":", HEADER_RETRY_COUNT);
            int startIdx = value.indexOf(key);
            if (startIdx == -1) {
                return 0;
            }
            
            startIdx += key.length();
            int endIdx = value.indexOf(",", startIdx);
            if (endIdx == -1) {
                endIdx = value.indexOf("}", startIdx);
            }
            
            if (endIdx > startIdx) {
                String countStr = value.substring(startIdx, endIdx).trim();
                return Integer.parseInt(countStr);
            }
        } catch (Exception e) {
            log.warn("解析重试次数失败: {}", e.getMessage());
        }
        
        return 0;
    }
    
    /**
     * 解析 Headers 获取最大重试次数
     */
    public static int getMaxRetry(Json headers) {
        if (headers == null || headers.value() == null || "{}".equals(headers.value())) {
            return 3; // 默认值
        }
        
        try {
            String value = headers.value();
            String key = String.format("\"%s\":", HEADER_MAX_RETRY);
            int startIdx = value.indexOf(key);
            if (startIdx == -1) {
                return 3;
            }
            
            startIdx += key.length();
            int endIdx = value.indexOf(",", startIdx);
            if (endIdx == -1) {
                endIdx = value.indexOf("}", startIdx);
            }
            
            if (endIdx > startIdx) {
                String maxStr = value.substring(startIdx, endIdx).trim();
                return Integer.parseInt(maxStr);
            }
        } catch (Exception e) {
            log.warn("解析最大重试次数失败: {}", e.getMessage());
        }
        
        return 3; // 默认值
    }
    
    /**
     * 解析 Headers 获取重试延迟时间
     */
    public static int getRetryDelaySeconds(Json headers) {
        if (headers == null || headers.value() == null || "{}".equals(headers.value())) {
            return 0; // 立即重试
        }
        
        try {
            String value = headers.value();
            String key = String.format("\"%s\":", HEADER_RETRY_DELAY);
            int startIdx = value.indexOf(key);
            if (startIdx == -1) {
                return 0;
            }
            
            startIdx += key.length();
            int endIdx = value.indexOf(",", startIdx);
            if (endIdx == -1) {
                endIdx = value.indexOf("}", startIdx);
            }
            
            if (endIdx > startIdx) {
                String delayStr = value.substring(startIdx, endIdx).trim();
                return Integer.parseInt(delayStr);
            }
        } catch (Exception e) {
            log.warn("解析重试延迟时间失败: {}", e.getMessage());
        }
        
        return 0; // 立即重试
    }
    
    /**
     * 获取重试跟踪 ID
     */
    public static String getRetryTraceId(Json headers) {
        if (headers == null || headers.value() == null || "{}".equals(headers.value())) {
            return "unknown";
        }
        
        try {
            String value = headers.value();
            String key = String.format("\"%s\":\"", HEADER_RETRY_TRACE_ID);
            int startIdx = value.indexOf(key);
            if (startIdx == -1) {
                return "unknown";
            }
            
            startIdx += key.length();
            int endIdx = value.indexOf("\"", startIdx);
            
            if (endIdx > startIdx) {
                return value.substring(startIdx, endIdx);
            }
        } catch (Exception e) {
            log.warn("解析重试跟踪 ID 失败: {}", e.getMessage());
        }
        
        return "unknown";
    }
    
    /**
     * 判断是否超过最大重试次数
     */
    public static boolean isMaxRetryExceeded(Json headers) {
        int retryCount = getRetryCount(headers);
        int maxRetry = getMaxRetry(headers);
        return retryCount >= maxRetry;
    }
    
    /**
     * 递增重试计数并返回更新后的 Headers
     */
    public static Json incrementRetryCount(Json headers) {
        if (headers == null || headers.value() == null || "{}".equals(headers.value())) {
            // 创建新的 headers
            return createInitialHeaders(3, 0);
        }
        
        try {
            String value = headers.value();
            int retryCount = getRetryCount(headers);
            String newValue = value.replaceFirst(
                String.format("\"%s\":%d", HEADER_RETRY_COUNT, retryCount),
                String.format("\"%s\":%d", HEADER_RETRY_COUNT, retryCount + 1)
            );
            
            return new Json(newValue);
        } catch (Exception e) {
            log.error("递增重试计数失败: {}", e.getMessage());
            return headers;
        }
    }
    
    /**
     * 计算下次重试的延迟时间（秒）
     * 采用指数退避策略：delay * 2^(retryCount-1)
     */
    public static int calculateNextRetryDelaySeconds(Json headers) {
        int baseDelay = getRetryDelaySeconds(headers);
        int retryCount = getRetryCount(headers);
        
        // 如果基础延迟为 0，立即重试
        if (baseDelay <= 0) {
            return 0;
        }
        
        // 指数退避
        long delay = baseDelay * (1L << (retryCount - 1));
        
        // 限制最大延迟为 3600 秒（1 小时）
        if (delay > 3600) {
            return 3600;
        }
        
        return (int) delay;
    }
}