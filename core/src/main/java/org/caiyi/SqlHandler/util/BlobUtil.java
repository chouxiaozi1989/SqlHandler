package org.caiyi.SqlHandler.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;

public class BlobUtil {
    public BlobUtil() {
    }

    public static void writeBytesToBlob(Blob blob, byte[] bytes) throws Exception {
        OutputStream os = null;

        try {
            if (blob == null) {
                throw new Exception("待写入的Blob对象为null，请检查!");
            }

            if (bytes == null) {
                throw new Exception("写入BLOB的bytes数组为null，请检查!");
            }

            os = blob.setBinaryStream(1L);
            os.write(bytes);
        } catch (Exception var11) {
            throw new Exception(var11);
        } finally {
            try {
                if (os != null) {
                    os.close();
                    os = null;
                }
            } catch (IOException var10) {
                throw new Exception(var10);
            }

        }

    }

    public static byte[] getBytes(Blob blob) throws Exception {
        InputStream is = null;
        byte[] b = null;
        if (blob == null) {
            throw new Exception("Blob对象为null，请检查!");
        } else {
            byte[] var3;
            try {
                if (blob.length() != 0L) {
                    is = blob.getBinaryStream();
                    b = new byte[(int) blob.length()];
                    is.read(b);
                    var3 = b;
                    return var3;
                }

                var3 = new byte[0];
            } catch (Exception var13) {
                throw new Exception(var13);
            } finally {
                try {
                    if (is != null) {
                        is.close();
                        is = null;
                    }
                } catch (IOException var12) {
                    throw new Exception(var12);
                }

            }

            return var3;
        }
    }
}