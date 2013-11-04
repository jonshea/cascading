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

package cascading.operation.bloom;

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedList;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class ScalableBloomFilter implements Serializable
  {
  private static final Logger LOG = LoggerFactory.getLogger( ScalableBloomFilter.class );

  public static enum Rate
    {
      SLOW( 2 ), FAST( 4 );

    public int rate;

    private Rate( int rate )
      {
      this.rate = rate;
      }
    }

  private final Rate growthRate;
  private final float errorProbabilityRatio;
  private final int initialCapacity;
  private final double falsePositiveProbability;

  private final LinkedList<BloomFilter<byte[]>> bloomFilters = new LinkedList<BloomFilter<byte[]>>();

  private long insertCount = 0;

  public ScalableBloomFilter( int initialCapacity )
    {
    this( Rate.SLOW, 0.9F, initialCapacity, 0.001F );
    }

  public ScalableBloomFilter( Rate growthRate, int initialCapacity )
    {
    this( growthRate, 0.9F, initialCapacity, 0.001F );
    }

  public ScalableBloomFilter( Rate growthRate, int initialCapacity, double falsePositiveProbability )
    {
    this( growthRate, 0.9F, initialCapacity, falsePositiveProbability );
    }

  public ScalableBloomFilter( Rate growthRate, float errorProbabilityRatio, int initialCapacity, double falsePositiveProbability )
    {
    this.growthRate = growthRate;
    this.errorProbabilityRatio = errorProbabilityRatio;
    this.initialCapacity = initialCapacity;
    this.falsePositiveProbability = falsePositiveProbability;

    this.bloomFilters.add( getTupleBloomFilter() );
    }

  public boolean put( byte[] tuple )
    {
    if( this.mightContain( tuple ) )
      return true;

    if( ( insertCount % 100 ) == 0 && bloomFilters.getLast().expectedFpp() > getScaledFPP( bloomFilters.size() ) )
      bloomFilters.add( getTupleBloomFilter() );

    bloomFilters.getLast().put( tuple );
    insertCount++;

    return false;
    }

  public boolean mightContain( byte[] tuple )
    {
    Iterator<BloomFilter<byte[]>> iterator = bloomFilters.descendingIterator();

    while( iterator.hasNext() )
      {
      if( iterator.next().mightContain( tuple ) )
        return true;
      }

    return false;
    }

  private BloomFilter<byte[]> getTupleBloomFilter()
    {
    int size = bloomFilters.size();
    double scaledCapacity = getScaledCapacity( size );
    double scaledFPP = getScaledFPP( size );

    LOG.info( "creating bloom filter #: {}, with scaledCapacity: {}, scaledFalsePositiveProbability: {}", bloomFilters.size() + 1, scaledCapacity, scaledFPP );

    return BloomFilter.create( getFunnel(), (int) scaledCapacity, scaledFPP );
    }

  private double getScaledCapacity( int size )
    {
    return initialCapacity * Math.pow( growthRate.rate, size );
    }

  private double getScaledFPP( int size )
    {
    return falsePositiveProbability * Math.pow( errorProbabilityRatio, size );
    }

  private Funnel<byte[]> getFunnel()
    {
    return Funnels.byteArrayFunnel();
    }
  }
