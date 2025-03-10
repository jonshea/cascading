/*
 * Copyright (c) 2016-2021 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
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

package cascading.nested.core;

import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Set;

import cascading.flow.FlowProcess;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.operation.OperationException;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import heretical.pointer.path.BaseNestedPointer;
import heretical.pointer.path.NestedPointer;
import heretical.pointer.path.NestedPointerCompiler;

/**
 * Class NestedGetFunction is the base class for {@link Function} implementations that want to simply retrieve
 * values in nested object trees and return them as tuple fields.
 * <p>
 * For every field named in the fieldDeclaration {@link Fields} argument, there must be a corresponding
 * {@code stringPointer} value.
 * <p>
 * If {@code failOnMissingNode} is {@code true} and the pointer returns a {@code null} value, the operation
 * will fail.
 * <p>
 * If the fieldDeclaration Fields instance declares a type information, the {@code nestedCoercibleType} will be used to coerce
 * any referenced child value to the expected field type.
 */
public class NestedGetFunction<Node, Result> extends NestedBaseOperation<Node, Result, Tuple> implements Function<Tuple>
  {
  protected final NestedPointer<Node, Result>[] pointers;
  protected final boolean failOnMissingNode;

  protected interface Setter<Node>
    {
    void set( int i, Node value );
    }

  /**
   * Constructor NestedGetFunction creates a new NestedGetFunction instance.
   *
   * @param nestedCoercibleType of NestedCoercibleType
   * @param fieldDeclaration    of Fields
   * @param failOnMissingNode   of boolean
   * @param stringPointers      of String...
   */
  public NestedGetFunction( NestedCoercibleType<Node, Result> nestedCoercibleType, Fields fieldDeclaration, boolean failOnMissingNode, String... stringPointers )
    {
    super( nestedCoercibleType, fieldDeclaration );
    this.failOnMissingNode = failOnMissingNode;

    verify( stringPointers );

    NestedPointerCompiler<Node, Result> compiler = getNestedPointerCompiler();

    this.pointers = new BaseNestedPointer[ stringPointers.length ];

    for( int i = 0; i < stringPointers.length; i++ )
      this.pointers[ i ] = compiler.nested( stringPointers[ i ] );
    }

  protected void verify( String[] stringPointers )
    {
    if( getFieldDeclaration().size() != stringPointers.length )
      throw new IllegalArgumentException( "pointers not same length as declared fields" );
    }

  @Override
  public void prepare( FlowProcess flowProcess, OperationCall<Tuple> operationCall )
    {
    operationCall.setContext( Tuple.size( pointers.length ) );
    }

  @Override
  public void operate( FlowProcess flowProcess, FunctionCall<Tuple> functionCall )
    {
    Tuple resultTuple = functionCall.getContext();
    Node argument = (Node) functionCall.getArguments().getObject( 0, getCoercibleType() );

    extractResult( resultTuple, argument );

    functionCall.getOutputCollector().add( resultTuple );
    }

  protected void extractResult( Tuple resultTuple, Node node )
    {
    extractResult( ( i, result ) -> setInto( resultTuple, i, result ), node );
    }

  protected void setInto( Tuple resultTuple, int i, Node result )
    {
    Type declaredType = getFieldDeclaration().getType( i );
    Object value = getCoercibleType().coerce( result, declaredType );

    resultTuple.set( i, value );
    }

  protected void extractResult( Setter<Node> resultSetter, Node node )
    {
    for( int i = 0; i < pointers.length; i++ )
      {
      Node result = pointers[ i ].at( node );

      if( failOnMissingNode && result == null )
        throw new OperationException( "node missing from json node tree: " + pointers[ i ] );

      resultSetter.set( i, result );
      }
    }

  protected static String[] asArray( Collection<String> values )
    {
    return values.toArray( new String[ values.size() ] );
    }

  protected static Fields asFields( Set<Fields> fields )
    {
    return fields.stream().reduce( Fields.NONE, Fields::append );
    }
  }
