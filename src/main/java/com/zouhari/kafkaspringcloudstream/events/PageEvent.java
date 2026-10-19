package com.zouhari.kafkaspringcloudstream.events;
import java.util.Date;


/**
 * @author ZOUHARI Dyae errahmane
 * @Pseudo dezy
 **/

public record PageEvent(String name, String user, Date date, long duration) {
}