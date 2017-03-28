/*
 * Copyright (c) 2016-2017 Chris K Wensel. All Rights Reserved.
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

package cascading.tuple;

import java.util.Objects;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import java.util.stream.Stream;

import cascading.flow.FlowProcess;
import cascading.tap.Tap;
import cascading.tuple.type.CoercibleType;

/**
 * TupleEntryStream provides helper methods to create {@link TupleEntry} {@link Stream} instances from
 * {@link Tap} instances.
 */
public class TupleEntryStream
  {
  @SuppressWarnings("unchecked")
  public static Stream<TupleEntry> entryStream( Tap tap, FlowProcess flowProcess )
    {
    Objects.requireNonNull( tap );

    return tap.entryStream( flowProcess );
    }

  @SuppressWarnings("unchecked")
  public static Stream<TupleEntry> entryStreamCopy( Tap tap, FlowProcess flowProcess )
    {
    Objects.requireNonNull( tap );

    return tap.tupleStreamCopy( flowProcess );
    }

  @SuppressWarnings("unchecked")
  public static Stream<TupleEntry> entryStream( Tap tap, FlowProcess flowProcess, Fields selector )
    {
    Objects.requireNonNull( tap );
    Objects.requireNonNull( selector );

    return tap.entryStream( flowProcess, selector );
    }

  @SuppressWarnings("unchecked")
  public static Stream<TupleEntry> entryStreamCopy( Tap tap, FlowProcess flowProcess, Fields selector )
    {
    Objects.requireNonNull( tap );
    Objects.requireNonNull( selector );

    return tap.tupleStreamCopy( flowProcess, selector );
    }

  @SuppressWarnings("unchecked")
  public static <R> Function<TupleEntry, ? extends R> fieldToObject( Fields fields )
    {
    Objects.requireNonNull( fields );

    return value -> (R) value.getObject( fields );
    }

  @SuppressWarnings("unchecked")
  public static <R> Function<TupleEntry, R> fieldToObject( Fields fields, CoercibleType<R> type )
    {
    Objects.requireNonNull( fields );
    Objects.requireNonNull( type );

    return value -> (R) value.getObject( fields, type );
    }

  @SuppressWarnings("unchecked")
  public static <R> Function<TupleEntry, R> fieldToObject( Fields fields, Class<R> type )
    {
    Objects.requireNonNull( fields );
    Objects.requireNonNull( type );

    return value -> (R) value.getObject( fields, type );
    }

  public static ToIntFunction<TupleEntry> fieldToInt( Fields fields )
    {
    Objects.requireNonNull( fields );

    return value -> value.getInteger( fields );
    }

  public static ToIntFunction<TupleEntry> fieldToInt( Comparable name )
    {
    Objects.requireNonNull( name );

    return value -> value.getInteger( name );
    }

  public static ToLongFunction<TupleEntry> fieldToLong( Fields fields )
    {
    Objects.requireNonNull( fields );

    return value -> value.getLong( fields );
    }

  public static ToLongFunction<TupleEntry> fieldToLong( Comparable name )
    {
    Objects.requireNonNull( name );

    return value -> value.getLong( name );
    }

  public static ToDoubleFunction<TupleEntry> fieldToDouble( Fields fields )
    {
    Objects.requireNonNull( fields );

    return value -> value.getDouble( fields );
    }

  public static ToDoubleFunction<TupleEntry> fieldToDouble( Comparable name )
    {
    Objects.requireNonNull( name );

    return value -> value.getDouble( name );
    }
  }
