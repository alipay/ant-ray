package io.ray.runtime.io;

import io.ray.runtime.util.MemoryBuffer;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class BufferOutputStream extends OutputStream {
  private final MemoryBuffer buffer;

  public BufferOutputStream(MemoryBuffer buffer) {
    this.buffer = buffer;
  }

  public void write(int b) {
    buffer.writeByte((byte) b);
  }

  public void write(byte[] bytes, int offset, int length) {
    buffer.writeBytes(bytes, offset, length);
  }

  public void write(ByteBuffer byteBuffer, int numBytes) {
    buffer.write(byteBuffer, numBytes);
  }

}
