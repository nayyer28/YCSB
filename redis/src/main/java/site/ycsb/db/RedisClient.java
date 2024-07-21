/**
 * Copyright (c) 2012 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

/**
 * Redis client binding for YCSB.
 *
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 */

package site.ycsb.db;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import queryMessage.Message;
import queryMessage.Message.Query;

/**
 * YCSB binding for <a href="http://redis.io/">Redis</a>.
 *
 * See {@code redis/README.md} for details.
 */
public class RedisClient extends DB {

  public int expectedOps;

  public static final String INDEX_KEY = "_indices";

  // private int totalOps = 0;

  private AtomicInteger totalOps = new AtomicInteger(0);

  private int moduBftClientPort = 10000;

  private Socket activeConnection;

  private OutputStream out;
  private InputStream in;

  public void init() throws DBException {
    Properties props = getProperties();
    expectedOps = Integer.parseInt(props.getProperty("expectedOps"));
    // System.out.println("expectedOps: " + expectedOps);

    try {
      activeConnection = new Socket("localhost", moduBftClientPort);
      //System.out.println("so timeout: " + activeConnection.getSoTimeout());
      out = activeConnection.getOutputStream();
      in = activeConnection.getInputStream();
      // System.out.println(String.format("Connection established with modubft client
      // at port %d", moduBftClientPort));
    } catch (Exception e) {
      throw new DBException(
          String.format("Failed to establish  connection with modubft client at port %d", moduBftClientPort), e);
    }
  }

  public void cleanup() throws DBException {
    return;
  }

  /*
   * Calculate a hash for a key to store it in an index. The actual return value
   * of this function is not interesting -- it primarily needs to be fast and
   * scattered along the whole space of doubles. In a real world scenario one
   * would probably use the ASCII values of the keys.
   */
  private double hash(String key) {
    return key.hashCode();
  }

  // XXX jedis.select(int index) to switch to `table`

  /*
   * @Override
   * public Status read(String table, String key, Set<String> fields,
   * Map<String, ByteIterator> result) {
   * if (fields == null) {
   * StringByteIterator.putAllAsByteIterators(result, jedis.hgetAll(key));
   * } else {
   * String[] fieldArray = (String[]) fields.toArray(new String[fields.size()]);
   * List<String> values = jedis.hmget(key, fieldArray);
   * 
   * Iterator<String> fieldIterator = fields.iterator();
   * Iterator<String> valueIterator = values.iterator();
   * 
   * while (fieldIterator.hasNext() && valueIterator.hasNext()) {
   * result.put(fieldIterator.next(),
   * new StringByteIterator(valueIterator.next()));
   * }
   * assert !fieldIterator.hasNext() && !valueIterator.hasNext();
   * }
   * return result.isEmpty() ? Status.ERROR : Status.OK;
   * }
   */

  @Override
  public Status read(String table, String key, Set<String> fields,
      Map<String, ByteIterator> result) {
    Status resp;
    if (fields == null) {
      resp = sendQuery("HGETALL", key);
    } else {
      String[] args = new String[2 + fields.size()];
      String[] fieldsArray = (String[]) fields.toArray();
      args[0] = "HMGET";
      args[1] = key;

      for (int i = 2; i < 2 + fields.size(); i++) {
        args[i] = fieldsArray[i - 2];
      }
      resp = sendQuery(args);
    }
    totalOps.incrementAndGet();
    checkStatus();
    return resp;
  }

  /*
   * @Override
   * public Status insert(String table, String key,
   * Map<String, ByteIterator> values) {
   * if (jedis.hmset(key, StringByteIterator.getStringMap(values))
   * .equals("OK")) {
   * jedis.zadd(INDEX_KEY, hash(key), key);
   * return Status.OK;
   * }
   * return Status.ERROR;
   * }
   */

  @Override
  public Status insert(String table, String key,
      Map<String, ByteIterator> values) {
    String[] args = new String[values.size() * 2 + 2];
    args[0] = "HMSET";
    args[1] = key;
    int index = 2;
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      args[index++] = entry.getKey();
      args[index++] = entry.getValue().toString();
    }
    sendQuery(args);
    Status resp = sendQuery("ZADD", INDEX_KEY, Double.toString(hash(key)), key);
    totalOps.incrementAndGet();
    checkStatus();
    return resp;

  }

  /*
   * @Override
   * public Status delete(String table, String key) {
   * return jedis.del(key) == 0 && jedis.zrem(INDEX_KEY, key) == 0 ? Status.ERROR
   * : Status.OK;
   * }
   */

  @Override
  public Status delete(String table, String key) {
    sendQuery("DEL", key);
    Status resp = sendQuery("ZREM", key);
    totalOps.incrementAndGet();
    checkStatus();
    return resp;

  }

  /*
   * @Override
   * public Status update(String table, String key,
   * Map<String, ByteIterator> values) {
   * return jedis.hmset(key, StringByteIterator.getStringMap(values))
   * .equals("OK") ? Status.OK : Status.ERROR;
   * }
   */

  @Override
  public Status update(String table, String key,
      Map<String, ByteIterator> values) {
    String[] args = new String[values.size() * 2 + 2];
    args[0] = "HMSET";
    args[1] = key;
    int index = 2;
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      args[index++] = entry.getKey();
      args[index++] = entry.getValue().toString();
    }
    Status resp = sendQuery(args);
    totalOps.incrementAndGet();
    checkStatus();
    return resp;

  }

  /*
   * @Override
   * public Status scan(String table, String startkey, int recordcount,
   * Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
   * Set<String> keys = jedis.zrangeByScore(INDEX_KEY, hash(startkey),
   * Double.POSITIVE_INFINITY, 0, recordcount);
   * 
   * HashMap<String, ByteIterator> values;
   * for (String key : keys) {
   * values = new HashMap<String, ByteIterator>();
   * read(table, key, fields, values);
   * result.add(values);
   * }
   * 
   * return Status.OK;
   * }
   */

  @Override
  public Status scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    throw new UnsupportedOperationException("Scan is not offered yet");
  }

  /*
   * private Status sendQuery(String... args) {
   * System.out.println(String.join(" ", args));
   * totalOps++;
   * if (totalOps == expectedOps) {
   * try {
   * System.out.println("Thread sleeping for 2s. Ops count: " + totalOps +
   * " expected ops: " + expectedOps);
   * Thread.sleep(2000);
   * } catch (Exception e) {
   * System.err.println("Error sending query");
   * }
   * 
   * totalOps = 0;
   * }
   * return Status.OK;
   * }
   */

  private Status sendQuery(String... args) {

    /*
     * if (totalOps.get() > 0) {
     * return Status.OK;
     * }
     */

    /*
     * System.out.println(args.length);
     */
    try {
      byte[] msg = transformArgsToProtoMessage(args);
      // System.out.println("sending bytes: " + msg.length + " totalOps=" + totalOps);

      byte[] length = ByteBuffer.allocate(4).putInt(msg.length).array();
      out.write(length);
      out.write(msg);
      out.flush();
      // System.out.println(String.join(" ", args));
      // System.out.println("Total sent: " + totalOps);

    } catch (Exception e) {
      e.printStackTrace();
    }
    return Status.OK;
  }

  private byte[] transformArgsToProtoMessage(String[] args) {
    Message.Query.Builder queryBuilder = Query.newBuilder();
    for (String s : args) {
      queryBuilder.addValues(s);
    }
    Query query = queryBuilder.build();
    byte[] data = query.toByteArray();
    return data;
  }

  private void checkStatus() {
    try {
      if (totalOps.get() == expectedOps) {
        int response;
        // System.out.println("Awaiting response");
        while ((response = in.read()) != -1) {
          // System.out.println("response is " + response);
          if (response == 1) {
            out.close();
            in.close();
            activeConnection.close();
            break;
          }
        }

        totalOps.set(0);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

}
