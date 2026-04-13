package bronya.pgsql.ext.mq.util;

import com.alibaba.fastjson2.JSONObject;
import lombok.experimental.UtilityClass;

@UtilityClass
public class SysPgMqUtil {

    public JSONObject parseValue(JSONObject message){
        if(message == null){
            return JSONObject.parseObject("{}");
        }
        return message.getJSONObject("value");
    }
}
