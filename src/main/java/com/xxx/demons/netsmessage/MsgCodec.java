package com.xxx.demons.netsmessage;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

import org.apache.commons.lang3.ArrayUtils;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MsgCodec {
    public boolean isAck;
    public boolean isPublish;
    public long ackSeq;
    public long seq;
    public boolean success;
    public String body;
    public int code;
    public String error;


    @Override
    public String toString() {
        if (success)
            return "Ack: " + isAck + ", ackSeq: " + ackSeq + ", publish: " + isPublish + ", seq: " + seq + ", result: successful, body: \"" + body + "\"";
        else
            return "Ack: " + isAck + ", ackSeq: " + ackSeq + ", publish: " + isPublish + ", seq: " + seq + ", result: failed, code: " + code + ", message: " + error;
    }

    public static byte[] encodeAck(long seq) {
        return Longs.toByteArray(seq);
    }

    public static MsgCodec decode(byte[] bytes) {
        try {
            MsgCodec message = new MsgCodec();
            if (bytes.length == 8) {
                message.isAck = true;
                message.ackSeq = Longs.fromByteArray(bytes);
                return message;
            }
            byte[] ackBytes = Arrays.copyOfRange(bytes, 0, 8);
            byte[] seqBytes = Arrays.copyOfRange(bytes, 8, 16);
            message.seq = Longs.fromByteArray(seqBytes);
            message.ackSeq = Longs.fromByteArray(ackBytes);
            byte headerTag = bytes[16];
            byte successTag = bytes[17];
            if (successTag == 0) {
                message.isPublish = headerTag == 1;
                message.success = true;
                byte[] bodyBytes = Arrays.copyOfRange(bytes, 18, bytes.length);
                message.body = new String(bodyBytes, "UTF-8");
            } else {
                message.success = false;
                byte[] errorCodeBytes = Arrays.copyOfRange(bytes, 18, 22);
                byte[] errorMessageBytes = Arrays.copyOfRange(bytes, 22, bytes.length);
                message.code = Ints.fromByteArray(errorCodeBytes);
                message.error = new String(errorMessageBytes, "UTF-8");
            }
            return message;
        } catch (UnsupportedEncodingException e) {
            log.error("", e);
            throw new RuntimeException(e.getMessage());
        }
    }

    public static byte[] encode(long ackSeq, long seq, String body) {
        return encode(ackSeq, seq, body, false);
    }

    public static byte[] encode(long ackSeq, long seq, String body, boolean isPublish) {
        try {
            byte[] ackSeqBytes = Longs.toByteArray(ackSeq);
            byte[] seqBytes = Longs.toByteArray(seq);
            byte headerTag = (byte) (isPublish ? 1 : 0);
            byte successTag = 0;
            byte[] bodyBytes = body.getBytes("UTF-8");

            byte[] add = ArrayUtils.addAll(ackSeqBytes, seqBytes);
            add = ArrayUtils.add(add, headerTag);
            add = ArrayUtils.add(add, successTag);
            add = ArrayUtils.addAll(add, bodyBytes);
            return add;
        } catch (UnsupportedEncodingException e) {
            log.error("", e);
            throw new RuntimeException(e.getMessage());
        }
    }

    public static byte[] encode(long ackSeq, long seq, int errorCode, String errorMessage) {
        try {
            byte[] ackSeqBytes = Longs.toByteArray(ackSeq);
            byte[] seqBytes = Longs.toByteArray(seq);
            byte headerTag = 0;// not support now
            byte successTag = 1;
            byte[] codeBytes = Ints.toByteArray(errorCode);
            byte[] messageBytes = errorMessage.getBytes("UTF-8");

            byte[] add = ArrayUtils.addAll(ackSeqBytes, seqBytes);
            add = ArrayUtils.add(add, headerTag);
            add = ArrayUtils.add(add, successTag);
            add = ArrayUtils.addAll(add, codeBytes);
            add = ArrayUtils.addAll(add, messageBytes);
            return add;
        } catch (UnsupportedEncodingException e) {
            log.error("", e);
            throw new RuntimeException(e.getMessage());
        }
    }

    public static void main(String[] args) {

        String text = "{\"agent\":{\"type\":\"dealer\",\"id\":0,\"channel\":0,\"name\":\"dealer2\",\"seq\":\"34dbeb5e-b405-41ba-9c69-1dca7d8158a0\"},\"timestamp\":1508383791793,\"version\":30065,\"push\":[{\"type\":\"ALL\",\"addition\":[{\"$\":[\"o.set\",\"cards\",{}]},{\"$\":[\"o.set\",\"cards_order\",[]]},{\"$\":[\"o.set\",\"score\",{\"banker\":0,\"player\":0,\"tie\":false,\"banker_pair\":false,\"player_pair\":false}]},{\"$\":[\"o.set\",\"round_id\",\"B1-171019112951\"]},{\"$\":[\"o.add\",\"stop_bet_ts\",1508383806793]},{\"$\":[\"o.set\",\"stage\",\"bet\"]}],\"groups\":{},\"group_addition\":{},\"excludeChannel\":[]}]}";
        long start = System.currentTimeMillis();
        for (int i = 0; i < 100000; i++) {
            byte[] successBytes = encode(100, 100, text);
            MsgCodec success = decode(successBytes);
//            System.out.println(success);
            byte[] failedBytes = encode(1000, -1, text);
            MsgCodec failed = decode(failedBytes);
//            System.out.println(failed);
        }
        long end = System.currentTimeMillis();
        System.out.println("byte use " + (end - start) + " ms");

        start = System.currentTimeMillis();
        for (int i = 0; i < 100000; i++) {
            String successBytes = 0 + "#" + 100 + "#" + text;
            String[] success = successBytes.split("#");
            String failedBytes = 1 + "#" + 100 + "#" + text;
            String[] failed = failedBytes.split("#");
        }
        end = System.currentTimeMillis();
        System.out.println("string use " + (end - start) + " ms");

    }

}
