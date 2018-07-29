package columnar;

import java.io.IOException;

import io.InputBuffer;
import io.OutputBuffer;

public class CompressedBlockDescriptor extends BlockDescriptor {
    int rowCount;
    int uncompressedSize;
    int lengthUnion;
    int lengthOffset;
    int lengthPayload;

    public CompressedBlockDescriptor() {

    }

    public CompressedBlockDescriptor(int rowCount, int uncompressedSize, int lengthOffset, int lengthPayload) {
        this(rowCount, uncompressedSize, 0, lengthOffset, lengthPayload);
    }

    public CompressedBlockDescriptor(int rowCount, int uncompressedSize, int lengthUnion, int lengthOffset,
            int lengthPayload) {
        this.rowCount = rowCount;
        this.uncompressedSize = uncompressedSize;
        this.lengthUnion = lengthUnion;
        this.lengthOffset = lengthOffset;
        this.lengthPayload = lengthPayload;
    }

    public int getUncompressedSize() {
        return uncompressedSize;
    }

    public int getCompressedSize() {
        return lengthUnion + lengthOffset + lengthPayload;
    }

    public void writeTo(OutputBuffer out) throws IOException {
        out.writeFixed32(rowCount);
        out.writeFixed32(uncompressedSize);
        out.writeFixed32(lengthUnion);
        out.writeFixed32(lengthOffset);
        out.writeFixed32(lengthPayload);
    }

    public void read(InputBuffer in) throws IOException {
        this.rowCount = in.readFixed32();
        this.uncompressedSize = in.readFixed32();
        this.lengthUnion = in.readFixed32();
        this.lengthOffset = in.readFixed32();
        this.lengthPayload = in.readFixed32();
    }

}
