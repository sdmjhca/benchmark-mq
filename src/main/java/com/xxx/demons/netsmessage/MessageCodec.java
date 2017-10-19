package com.xxx.demons.netsmessage;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

import org.apache.commons.lang3.ArrayUtils;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MessageCodec {
    public boolean success;
    public long seq;
    public String body;
    public int errorCode;
    public String errorMessage;

    @Override
    public String toString() {
        if (success)
            return "seq: " + seq + ", result: successful, body: \"" + body + "\"";
        else
            return "seq: " + seq + ", result: failed, code: " + errorCode + ", message: \"" + errorMessage + "\"";
    }

    public static MessageCodec decode(byte[] bytes) {
        try {
            MessageCodec message = new MessageCodec();
            byte[] seqBytes = Arrays.copyOfRange(bytes, 0, 8);
            message.seq = Longs.fromByteArray(seqBytes);
            byte headerTag = bytes[8];// not support now
            byte successTag = bytes[9];
            if (successTag == 0) {
                message.success = true;
                byte[] bodyBytes = Arrays.copyOfRange(bytes, 10, bytes.length);
                message.body = new String(bodyBytes, "UTF-8");
            } else {
                message.success = false;
                byte[] errorCodeBytes = Arrays.copyOfRange(bytes, 10, 14);
                byte[] errorMessageBytes = Arrays.copyOfRange(bytes, 14, bytes.length);
                message.errorCode = Ints.fromByteArray(errorCodeBytes);
                message.errorMessage = new String(errorMessageBytes, "UTF-8");
            }
            return message;
        } catch (UnsupportedEncodingException e) {
            log.error("", e);
            throw new RuntimeException(e.getMessage());
        }
    }

    public static byte[] encode(long seq, String body) {
        try {
            byte[] seqBytes = Longs.toByteArray(seq);
            byte headerTag = 0;
            byte successTag = 0;
            byte[] bodyBytes = body.getBytes("UTF-8");

            byte[] add = ArrayUtils.add(seqBytes, headerTag);
            add = ArrayUtils.add(add, successTag);
            add = ArrayUtils.addAll(add, bodyBytes);
            return add;
        } catch (UnsupportedEncodingException e) {
            log.error("", e);
            throw new RuntimeException(e.getMessage());
        }
    }

    public static byte[] encode(long seq, int errorCode, String errorMessage) {
        try {
            byte[] seqBytes = Longs.toByteArray(seq);
            byte headerTag = 0;
            byte successTag = 1;
            byte[] codeBytes = Ints.toByteArray(errorCode);
            byte[] messageBytes = errorMessage.getBytes("UTF-8");

            byte[] add = ArrayUtils.add(seqBytes, headerTag);
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
        byte[] successBytes = encode(100, "啦啦啦啦啦");
        MessageCodec success = decode(successBytes);
        System.out.println(success);


        byte[] failedBytes = encode(1000, -1, "not ok");
        MessageCodec failed = decode(failedBytes);
        System.out.println(failed);
    }

}
