/*
 * Copyright (c) 2007-2017 Xplenty, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cascading.tuple.hadoop.io;

import java.io.ByteArrayInputStream;

/**
 *
 */
public class BufferedInputStream extends ByteArrayInputStream
  {
  private static final byte[] ZERO_BYTES = new byte[]{};

  public BufferedInputStream()
    {
    super( ZERO_BYTES );
    }

  public void reset( byte[] input, int start, int length )
    {
    this.buf = input;
    this.count = start + length;
    this.mark = start;
    this.pos = start;
    }

  public byte[] getBuffer()
    {
    return buf;
    }

  public int getPosition()
    {
    return pos;
    }

  public int getLength()
    {
    return count;
    }

  public void clear()
    {
    this.buf = ZERO_BYTES;
    this.count = 0;
    this.mark = 0;
    this.pos = 0;
    }
  }

