package bronya.pgsql.ext.mq.util;

import bronya.pgsql.ext.mq.consumer.ListenerMethodInfo;
import cn.hutool.v7.core.text.StrUtil;
import cn.hutool.v7.extra.spring.SpringUtil;
import com.alibaba.cola.exception.BizException;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.andreaesposito.pgmq.jdbc.client.Json;

import java.lang.reflect.Method;

@Slf4j
@UtilityClass
public class SysPgMqUtil {

    /**
     * 处理消息 - 通过反射调用实际的监听方法
     */
    public Boolean processMessage(ListenerMethodInfo methodInfo, Json message) {
        try {
            // 从 Spring 上下文获取 Bean
            Object bean = SpringUtil.getBean(methodInfo.beanName());

            // 获取方法
            Method method = findMethod(bean.getClass(), methodInfo.methodName(), methodInfo.msgDtoClass());

            // 将 JSON 消息转换为 DTO 对象
            Object dto = JSON.parseObject(message.value(), methodInfo.msgDtoClass());

            // 调用方法
            method.invoke(bean, dto);

            log.info("消息处理成功: 队列={}, 类={}, 方法={}", methodInfo.queueName(), methodInfo.className(), methodInfo.methodName());
            return true;
        } catch (Exception e) {
            log.error("消息处理失败: 队列={}, 类={}, 方法={}, 错误={}", methodInfo.queueName(), methodInfo.className(), methodInfo.methodName(), e.getMessage(), e);
            return false;
        }
    }

    /**
     * 查找匹配的方法
     */
    public Method findMethod(Class<?> clazz, String methodName, Class<?> paramType) {
        for (Method method : clazz.getDeclaredMethods()) {
            if (method.getName().equals(methodName) && method.getParameterCount() == 1) {
                if (method.getParameterTypes()[0].isAssignableFrom(paramType)) {
                    method.setAccessible(true);
                    return method;
                }
            }
        }
        throw new BizException(StrUtil.format("未找到方法: {}.{}", clazz.getSimpleName(), methodName));
    }

    public JSONObject parseValue(JSONObject message){
        if(message == null){
            return JSONObject.parseObject("{}");
        }
        return message.getJSONObject("value");
    }

    /**
     * 从 MessageRecord 中提取 Headers
     */
    public Json extractHeadersFromMessageRecord(org.andreaesposito.pgmq.jdbc.client.MessageRecord messageRecord) {
        try {
            // 尝试通过反射获取 headers 字段
            var headersField = messageRecord.getClass().getMethod("headers");
            Object headersObj = headersField.invoke(messageRecord);

            if (headersObj instanceof Json) {
                return (Json) headersObj;
            } else if (headersObj != null) {
                return new Json(headersObj.toString());
            }
        } catch (Exception e) {
            // 如果无法获取 headers，返回空对象
        }

        return new Json("{}");
    }

    // 修复：更改返回类型为 MessageProcessResult，但保留原有参数

    /**
     * 从 Headers 中解析重试次数
     */
    public int getRetryCountFromHeaders(Json headers) {
        if (headers == null || headers.value() == null || "{}".equals(headers.value())) {
            return 0;
        }

        try {
            String value = headers.value();
            // 简单的 JSON 解析，查找 retry-count 字段
            String key = "\"x-retry-count\":";
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
     * 从 Headers 中解析跟踪 ID
     */
    public String getTraceIdFromHeaders(Json headers) {
        if (headers == null || headers.value() == null || "{}".equals(headers.value())) {
            return "unknown";
        }

        try {
            String value = headers.value();
            String key = "\"x-retry-trace-id\":\"";
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
            log.warn("解析跟踪 ID 失败: {}", e.getMessage());
        }

        return "unknown";
    }
}
