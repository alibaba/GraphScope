package com.alibaba.maxgraph.v2.common.util;

import java.security.SecureRandom;
import java.util.Base64;
import java.util.UUID;

public class UuidUtils {

    private static class SecureRandomHolder {
        static final SecureRandom INSTANCE = new SecureRandom();
    }

    public static String getBase64UUIDString() {
        byte[] bytes = new byte[16];
        SecureRandomHolder.INSTANCE.nextBytes(bytes);
        // UUID v4. (http://www.ietf.org/rfc/rfc4122.txt)
        // clear version of 4 msb
        bytes[6]  &= 0x0f;
        // set version to 4
        bytes[6]  |= 0x40;
        // clear variant of 2 msb
        bytes[8]  &= 0x3f;
        // set variant
        bytes[8]  |= 0x80;
        // return base64 encoding
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }
}
