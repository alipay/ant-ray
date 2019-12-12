package org.ray.streaming.runtime.core.queue.transfer;

import java.util.Collection;
import java.util.List;
import java.util.Random;

import javax.xml.bind.DatatypeConverter;

import org.ray.api.id.ActorId;

import com.google.common.base.Preconditions;

public class ChannelUtils {
  public static byte[] qidStrToBytes(String qid) {
    byte[] qidBytes = DatatypeConverter.parseHexBinary(qid.toUpperCase());
    assert qidBytes.length == ChannelID.ID_LENGTH;
    return qidBytes;
  }

  public static String qidBytesToString(byte[] qid) {
    assert qid.length == ChannelID.ID_LENGTH;
    return DatatypeConverter.printHexBinary(qid).toLowerCase();
  }

  public static byte[][] stringQueueIdListToByteArray(Collection<String> queueIds) {
    byte[][] res = new byte[queueIds.size()][20];
    int i = 0;
    for (String s : queueIds) {
      res[i] = ChannelUtils.qidStrToBytes(s);
      i++;
    }
    return res;
  }

  public static long[] longToPrimitives(List<Long> arr) {
    long[] res = new long[arr.size()];
    for (int i = 0; i < arr.size(); ++i) {
      res[i] = arr.get(i);
    }
    return res;
  }

  public static String getRandomQueueId() {
    StringBuilder sb = new StringBuilder();
    Random random = new Random();
    for (int i = 0; i < ChannelID.ID_LENGTH * 2; ++i) {
      sb.append((char) (random.nextInt(6) + 'A'));
    }
    return sb.toString();
  }

  /**
   * generate queue name, which must be 20 character
   *
   * @param fromTaskId actor which write into queue
   * @param toTaskId   actor which read from queue
   * @return queue name
   */
  public static String genQueueName(int fromTaskId, int toTaskId, long ts) {
    /*
      | Queue Head | Timestamp | Empty | From  |  To    |
      | 8 bytes    |  4bytes   | 4bytes| 2bytes| 2bytes |
    */
    Preconditions.checkArgument(fromTaskId < Short.MAX_VALUE,
        "fromTaskId %d is larger than %d", fromTaskId, Short.MAX_VALUE);
    Preconditions.checkArgument(toTaskId < Short.MAX_VALUE,
        "toTaskId %d is larger than %d", fromTaskId, Short.MAX_VALUE);
    byte[] queueName = new byte[20];

    for (int i = 11; i >= 8; i--) {
      queueName[i] = (byte) (ts & 0xff);
      ts >>= 8;
    }

    queueName[16] = (byte) ((fromTaskId & 0xffff) >> 8);
    queueName[17] = (byte) (fromTaskId & 0xff);
    queueName[18] = (byte) ((toTaskId & 0xffff) >> 8);
    queueName[19] = (byte) (toTaskId & 0xff);

    return ChannelUtils.qidBytesToString(queueName);
  }

  public static byte[][] actorIdListToByteArray(Collection<ActorId> actorIds) {
    byte[][] res = new byte[actorIds.size()][ActorId.fromRandom().size()];
    int i = 0;
    for (ActorId id : actorIds) {
      res[i] = id.getBytes();
      i++;
    }
    return res;
  }
}

