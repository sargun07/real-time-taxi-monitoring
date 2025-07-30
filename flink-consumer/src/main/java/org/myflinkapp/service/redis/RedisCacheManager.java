package org.myflinkapp.service.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;

public class RedisCacheManager {
    private static RedisCacheManager instance;
    private static JedisPool pool;

    private RedisCacheManager() {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(128);
        pool = new JedisPool(config, "redis", 6379);
    }

    public static synchronized RedisCacheManager getInstance() {
        if (instance == null) {
            instance = new RedisCacheManager();
        }
        return instance;
    }

    public void updateLocation(String key, double latitude, double longitude) {
        try (Jedis jedis = pool.getResource()) {
            jedis.geoadd("taxi_locations", longitude, latitude, key);
        } catch (Exception e) {
            System.err.println("Failed to update location for " + key);
            e.printStackTrace();
        }
    }

    public void updateStats(String taxiId, double latitude, double longitude, long timestamp, double averageSpeed, double totalDistanceKm) {
        String key = "Taxi:" + taxiId;
        try (Jedis jedis = pool.getResource()) {
            jedis.hset(key, "lastUpdateTime", String.valueOf(timestamp));
            updateLocation(key, latitude, longitude);
            jedis.hset(key, "totalDistance", String.valueOf(totalDistanceKm));
            if (!Double.isNaN(averageSpeed) && !Double.isInfinite(averageSpeed) && averageSpeed > 0) {
                jedis.hset(key, "averageSpeed", String.valueOf(averageSpeed));
            }
        } catch (Exception e) {
            System.err.println("Failed to update stats for taxiId: " + taxiId);
            e.printStackTrace();
        }
    }

    public void updateCurrentSpeed(String taxiId, double currentSpeed) {
        String key = "Taxi:" + taxiId;
        try (Jedis jedis = pool.getResource()) {
            jedis.hset(key, "currentSpeed", String.valueOf(currentSpeed));
        }catch (Exception e) {
            System.err.println("Failed to update current speed for: " + taxiId);
            e.printStackTrace();
        }
    }

    public void updateSpeedingStatus(String taxiId, boolean isSpeeding, double speedKmph) {
        String key = "Taxi:" + taxiId;

        try (Jedis jedis = pool.getResource()) {
            jedis.hset(key, "speeding", isSpeeding ? "true" : "false");
            jedis.hset(key, "speedingSpeed", isSpeeding ? String.valueOf(speedKmph) : "");
            if (isSpeeding){
                jedis.sadd("violations:speeding_incidents", "Taxi:" + taxiId);
            }else{
                jedis.srem("violations:speeding_incidents", "Taxi:" + taxiId);
            }
        } catch (Exception e) {
            System.err.println("Failed to update speeding info for taxiId=" + taxiId);
            e.printStackTrace();
        }
    }

    public void removeTaxi(String taxiId) {
        try (Jedis jedis = pool.getResource()) {
            jedis.del("Taxi:" + taxiId);
        }   
    }

    public void setTenKmCrossed(String taxiId, boolean crossed) {
        String key = "Taxi:" + taxiId;
        try (Jedis jedis = pool.getResource()) {
            jedis.hset(key, "tenKmCrossed", crossed ? "true" : "false");
            if(crossed){
                jedis.sadd("violations:area_10km", "Taxi:" + taxiId);
            }else{
                jedis.srem("violations:area_10km", "Taxi:" + taxiId);
            }
        } catch (Exception e) {
            System.err.println("Failed to update 10km status for taxiId=" + taxiId);
            e.printStackTrace();
        }
    }

    public void pushLog(String message) {
        try (Jedis jedis = pool.getResource()) {
            jedis.lpush("logs:events",message);
        } catch (Exception e) {
            System.err.println("Failed to push log message to Redis.");
            e.printStackTrace();
        }
    }

    public void markTaxiActive(String taxiId) {
        try (Jedis jedis = pool.getResource()) {
            String key = "Taxi:" + taxiId;
            String currentStatus = jedis.hget(key, "status");

            if (!"active".equals(currentStatus)) {
                jedis.hset(key, "status", "active");

                if ("inactive".equals(currentStatus)) {
                    if (jedis.srem("taxis:inactive", "Taxi:" + taxiId) == 1) {
                        jedis.decr("count:inactiveTaxis");
                    }
                    jedis.lpush("logs:events", "[Status Update] Taxi Id:" + taxiId + " is now ACTIVE (previously inactive).");

                } else if ("removed".equals(currentStatus)) {
                    if (jedis.srem("taxis:removed", "Taxi:" + taxiId) == 1) {
                        jedis.decr("count:removedTaxis");
                    }
                    jedis.lpush("logs:events", "[Status Update] Taxi Id:" + taxiId + " is now ACTIVE (after being removed due to area violation).");

                } else if ("halted".equals(currentStatus)) {
                    if (jedis.srem("taxis:halted", "Taxi:" + taxiId) == 1) {
                        jedis.decr("count:haltedTaxis");
                    }
                    jedis.lpush("logs:events", "[Status Update] Taxi Id:" + taxiId + " is now ACTIVE (previously halted).");

                } else {
                    jedis.lpush("logs:events", "[Status Update] Taxi Id:" + taxiId + " is now ACTIVE.");
                }

                if (jedis.sadd("taxis:active", "Taxi:" + taxiId) == 1) {
                    jedis.incr("count:activeTaxis");
                }
            }
        } catch (Exception e) {
            System.err.println("Failed to mark taxi as active: " + taxiId);
            e.printStackTrace();
        }
    }


    public void markTaxiInactive(String taxiId) {
        try (Jedis jedis = pool.getResource()) {
            String key = "Taxi:" + taxiId;
            String currentStatus = jedis.hget(key, "status");

            if (!"inactive".equals(currentStatus)) {
                jedis.hset(key, "status", "inactive");

                // Update logs and counters based on previous status
                if ("active".equals(currentStatus)) {
                    jedis.decr("count:activeTaxis");
                    jedis.srem("taxis:active", "Taxi:" + taxiId);
                } else if ("removed".equals(currentStatus)) {
                    jedis.decr("count:removedTaxis");
                    jedis.srem("taxis:removed", "Taxi:" + taxiId);
                } else if ("halted".equals(currentStatus)) {
                    jedis.decr("count:haltedTaxis");
                    jedis.srem("taxis:halted", "Taxi:" + taxiId);
                } else {
                    jedis.lpush("logs:events", "[Status Update] " + taxiId + "is now INACTIVE.");
                }

                jedis.sadd("taxis:inactive", "Taxi:" + taxiId);
                jedis.incr("count:inactiveTaxis");
                
            }

        } catch (Exception e) {
            System.err.println("Failed to mark taxi as inactive: " + taxiId);
            e.printStackTrace();
        }
    }


    public void markTaxiRemoved(String taxiId, int areaViolationCount) {
        try (Jedis jedis = pool.getResource()) {
            String key = "Taxi:" + taxiId;
            String currentStatus = jedis.hget(key, "status");

            if (!"removed".equals(currentStatus)) {
                jedis.hset(key, "status", "removed");

                // Update counters and sets based on current status
                if ("active".equals(currentStatus)) {
                    jedis.decr("count:activeTaxis");
                    jedis.srem("taxis:active", "Taxi:" + taxiId);
                } else if ("inactive".equals(currentStatus)) {
                    jedis.decr("count:inactiveTaxis");
                    jedis.srem("taxis:inactive", "Taxi:" + taxiId);
                } else if ("halted".equals(currentStatus)) {
                    jedis.decr("count:haltedTaxis");
                    jedis.srem("taxis:halted", "Taxi:" + taxiId);
                }

                // Add to removed set and counter
                jedis.sadd("taxis:removed", "Taxi:" + taxiId);
                jedis.incr("count:removedTaxis");

                // Log the removal event
                
                jedis.lpush("logs:events",
                    "[Removal Alert] Taxi Id:" + taxiId + " removed after exceeding 15km zone. Area violations: " + areaViolationCount + ".");
            }
        } catch (Exception e) {
            System.err.println("Failed to mark taxi as removed: " + taxiId);
            e.printStackTrace();
        }
    }


    public void setTaxiHaltedStatus(String taxiId, boolean isHalted) {
    try (Jedis jedis = pool.getResource()) {
        String key = "Taxi:" + taxiId;
        String currentStatus = jedis.hget(key, "status");

        if ("removed".equals(currentStatus)) {
            // Do not update halted/active if taxi is already removed
            return;
        }

        if (isHalted) {
            if (!"halted".equals(currentStatus)) {
                jedis.hset(key, "status", "halted");

                // Remove from active/inactive
                if ("active".equals(currentStatus)) {
                    jedis.decr("count:activeTaxis");
                    jedis.srem("taxis:active", "Taxi:" + taxiId);
                } else if ("inactive".equals(currentStatus)) {
                    jedis.decr("count:inactiveTaxis");
                    jedis.srem("taxis:inactive", "Taxi:" + taxiId);
                }

                // Add to halted
                jedis.sadd("taxis:halted", "Taxi:" + taxiId);
                jedis.incr("count:haltedTaxis");

                jedis.lpush("logs:events", 
                    "[Warning] Taxi ID:" + taxiId + " has been halted for more than 3 minutes.");
            }
        } else {
            if (!"active".equals(currentStatus)) {
                jedis.hset(key, "status", "active");

                // If coming from "halted", remove from halted set
                if ("halted".equals(currentStatus)) {
                    jedis.decr("count:haltedTaxis");
                    jedis.srem("taxis:halted", "Taxi:" + taxiId);
                } else if ("inactive".equals(currentStatus)) {
                    jedis.decr("count:inactiveTaxis");
                    jedis.srem("taxis:inactive", "Taxi:" + taxiId);
                }

                // Add to active
                jedis.sadd("taxis:active", "Taxi:" + taxiId);
                jedis.incr("count:activeTaxis");

                jedis.lpush("logs:events", 
                    "Taxi ID:" + taxiId + " resumed movement and is marked as active.");
            }
        }
    } catch (Exception e) {
        System.err.println("Failed to update halt status for taxi: " + taxiId);
        e.printStackTrace();
    }
}


    public void updateTotalDistance(String taxiId, double newDistanceKm) {
        try (Jedis jedis = pool.getResource()) {
            String key = "Taxi:" + taxiId;
            String prev = jedis.hget(key, "totalDistance");

            double previousDistance = (prev != null) ? Double.parseDouble(prev) : 0.0;
            double delta = newDistanceKm - previousDistance;

            // Update the taxi's own total distance
            jedis.hset(key, "totalDistance", String.valueOf(newDistanceKm));

            if (delta > 0) {
                jedis.incrByFloat("count:totalDistanceAllTaxis", delta);
            }
        }catch (Exception e) {
            System.err.println("Failed to update total distance of taxi : " + taxiId);
            e.printStackTrace();
        }
    }

    public void resetTotalDistance(String taxiId) {
        try (Jedis jedis = pool.getResource()) {
            String key = "Taxi:" + taxiId;
            jedis.hset(key, "totalDistance", "0.0");
        }catch (Exception e) {
            System.err.println("Failed to reset total distance for: " + taxiId);
            e.printStackTrace();
        }
    }

    public String getTaxiStatus(String taxiId) {
        try (Jedis jedis = pool.getResource()) {
            String key = "Taxi:" + taxiId;
            return jedis.hget(key, "status");
        } catch (Exception e) {
            System.err.println("Failed to get status for taxiId: " + taxiId);
            e.printStackTrace();
            return null;
        }
    }

    public void clearTaxiData() {
        try (Jedis jedis = pool.getResource()) {
            Set<String> keys = jedis.keys("taxi:*");
            for (String key : keys) {
                jedis.del(key);
            }

            // Also clear sets and counters if needed
            jedis.del("taxis:speeding", "taxis:violated10km", "taxis:active", "taxis:removed");
            jedis.del("count:activeTaxis", "count:inactiveTaxis", "count:removedTaxis");
            jedis.del("logs:events");
        } catch (Exception e) {
            System.err.println("Failed to clear Redis keys.");
            e.printStackTrace();
        }
    }

    public void updateTrafficgrid(String key, double bottomLeftLat, double bottomLeftLon, double topRightLat, double topRightLon, double avgSpeed, int taxiCount, String trafficStatus, long timestamp){
        try (Jedis jedis = pool.getResource()) {
            Map<String, String> gridData = new HashMap<>();
            gridData.put("bottomLeftLat", String.valueOf(bottomLeftLat));
            gridData.put("bottomLeftLon", String.valueOf(bottomLeftLon));
            gridData.put("topRightLat", String.valueOf(topRightLat));
            gridData.put("topRightLon", String.valueOf(topRightLon));
            gridData.put("avgSpeed", String.format("%.2f", avgSpeed));
            gridData.put("taxiCount", String.valueOf(taxiCount));
            gridData.put("trafficStatus", trafficStatus);
            gridData.put("lastUpdated", String.valueOf(timestamp));

            jedis.hset(key, gridData);
            
            jedis.zadd("updatedGrids", timestamp, key);
        }
        catch (Exception e) {
            System.err.println("Failed to update the traffic grid.");
            e.printStackTrace();
        }
    }

    public static void shutdown() {
        if (pool != null && !pool.isClosed()) {
            pool.close();
            System.out.println("Redis pool closed.");
        }
    }

}
