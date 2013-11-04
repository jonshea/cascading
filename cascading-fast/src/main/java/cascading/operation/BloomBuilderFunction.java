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

import java.io.IOException;

import cascading.CascadingException;
import cascading.flow.FlowProcess;
import cascading.operation.bloom.ScalableBloomFilter;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

/**
 *
 */
public class BloomBuilderFunction extends BaseOperation<BloomBuilderFunction.Context> implements Function<BloomBuilderFunction.Context>
  {
  private final ScalableBloomFilter.Rate growthRate;
  private final float errorProbabilityRatio;
  private final int initialCapacity;
  private final double falsePositiveRate;

  protected static class Context
    {
    public ScalableBloomFilter filter;
    public TupleBytesOutputStream bytes;
    }

  public BloomBuilderFunction( Fields fieldDeclaration, ScalableBloomFilter.Rate growthRate, float errorProbabilityRatio, int initialCapacity, double falsePositiveRate )
    {
    super( fieldDeclaration );

    this.growthRate = growthRate;
    this.errorProbabilityRatio = errorProbabilityRatio;
    this.initialCapacity = initialCapacity;
    this.falsePositiveRate = falsePositiveRate;

    }

  @Override
  public void prepare( FlowProcess flowProcess, OperationCall<BloomBuilderFunction.Context> operationCall )
    {
    Context context = new Context();

    context.filter = new ScalableBloomFilter( growthRate, errorProbabilityRatio, initialCapacity, falsePositiveRate );
    context.bytes = new TupleBytesOutputStream();

    operationCall.setContext( context );
    }

  @Override
  public void operate( FlowProcess flowProcess, FunctionCall<BloomBuilderFunction.Context> functionCall )
    {
    TupleEntry arguments = functionCall.getArguments();
    Context context = functionCall.getContext();

    byte[] bytes = writeReturnBytes( context.bytes, arguments );

    context.filter.put( bytes );
    }

  private byte[] writeReturnBytes( TupleBytesOutputStream bytes, TupleEntry arguments )
    {
    try
      {
      bytes.writeTuple( arguments.getTuple() );
      bytes.flush();

      return bytes.toByteArray();
      }
    catch( IOException exception )
      {
      throw new CascadingException( "unable to write tuple bytes", exception );
      }
    finally
      {
      bytes.reset();
      }
    }

  @Override
  public void flush( FlowProcess flowProcess, OperationCall<BloomBuilderFunction.Context> operationCall )
    {

    }
  }
