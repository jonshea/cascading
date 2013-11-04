/*
 * Copyright (c) 2007-2009 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Cascading is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Cascading is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Cascading.  If not, see <http://www.gnu.org/licenses/>.
 */

package cascading.operation;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Type;

import cascading.tuple.Tuple;
import cascading.tuple.coerce.Coercions;
import com.google.common.io.Flushables;

/**
 *
 */
class TupleBytesOutputStream extends DataOutputStream
  {
  public TupleBytesOutputStream()
    {
    this( new ByteArrayOutputStream() );
    }

  public TupleBytesOutputStream( ByteArrayOutputStream outputStream )
    {
    super( outputStream );
    }

  public void writeTuple( Type[] types, Tuple tuple ) throws IOException
    {
    for( int i = 0; i < types.length; i++ )
      {
      Type type = Coercions.asNonPrimitive( Coercions.asClass( types[ i ] ) );
      Object value = tuple.getObject( i );

      if( value == null )
        continue;

      writeTypedObject( type, value );
      }
    }

  public void writeTuple( Tuple tuple ) throws IOException
    {
    for( int i = 0; i < tuple.size(); i++ )
      {
      Object value = tuple.getObject( i );

      if( value == null )
        continue;

      Class type = value.getClass();

      writeTypedObject( type, value );
      }
    }

  private void writeTypedObject( Type type, Object value ) throws IOException
    {
    // todo: byte and byte[]

    if( String.class == type )
      writeUTF( (String) value );
    else if( Integer.class == type )
      writeInt( (Integer) value );
    else if( Long.class == type )
      writeLong( (Long) value );
    else if( Float.class == type )
      writeFloat( (Float) value );
    else if( Double.class == type )
      writeDouble( (Double) value );
    else if( Boolean.class == type )
      writeBoolean( (Boolean) value );
    else if( Short.class == type )
      writeShort( (Short) value );
    else if( Character.class == type )
      writeChar( (Character) value );
    else if( Tuple.class == type )
      writeTuple( (Tuple) value );
    else
      throw new IOException( "unknown type: " + Coercions.getTypeName( type ) + ", cannot pack into byte array" );
    }

  public ByteArrayOutputStream getWrapped()
    {
    return (ByteArrayOutputStream) out;
    }

  public void reset()
    {
    Flushables.flushQuietly( this );
    getWrapped().reset();
    }

  public byte[] toByteArray()
    {
    Flushables.flushQuietly( this );
    return getWrapped().toByteArray();
    }
  }
