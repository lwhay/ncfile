/**
 * 
 */
package codec;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.trevni.TrevniRuntimeException;

import metadata.MetaData;

/**
 * @author Michael
 *
 */
public class CodecFactory {
    private final Codec codec;

    @SuppressWarnings("rawtypes")
    public CodecFactory(MetaData meta) {
        String name = meta.getCodec();
        if (name == null || "null".equals(name))
            this.codec = new NullCodec();
        else if ("deflate".equals(name))
            this.codec = new DeflateCodec();
        else if ("snappy".equals(name))
            this.codec = new SnappyCodec();
        else if ("bzip2".equals(name))
            this.codec = new BZip2Codec();
        else
            throw new TrevniRuntimeException("Unknown codec: " + name);
    }

    /**
     * Compress data
     */
    public ByteBuffer compress(ByteBuffer uncompressedData) throws IOException {
        return codec.compress(uncompressedData);
    }

    /**
     * Decompress data
     */
    public ByteBuffer decompress(ByteBuffer compressedData) throws IOException {
        return this.codec.decompress(compressedData);
    }
}
