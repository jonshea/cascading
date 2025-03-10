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

package cascading;

import cascading.util.Util;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

/**
 *
 */
public class UtilTest
  {
  @Test
  public void testParseJarPath()
    {
    String[] paths = new String[]{
      "name.jar",
      "foo/bar/name.jar",
      "/foo/bar/name.jar",
      "name-3.5.7.jar",
      "foo/bar/name-3.5.7.jar",
      "/foo/bar/name-3.5.7.jar",
      "name-20101201.jar",
      "foo/bar/name-20101201.jar",
      "/foo/bar/name-20101201.jar",
      "name-test-3.0.5.RELEASE.jar",
      "foo/bar/name-test-3.0.5.RELEASE.jar",
      "/foo/bar/name-test-3.0.5.RELEASE.jar",
      "name-test-2.0.0-wip-dev.jar",
      "foo/bar/name-test-2.0.0-wip-dev.jar",
      "/foo/bar/name-test-2.0.0-wip-dev.jar",
      "file:///C:\\foo\\bar\\name-test-2.0.0-wip-dev.jar"
    };

    String[] names = new String[]{
      "name",
      "name",
      "name",
      "name",
      "name",
      "name",
      "name",
      "name",
      "name",
      "name-test",
      "name-test",
      "name-test",
      "name-test",
      "name-test",
      "name-test",
      "name-test"
    };

    String[] versions = new String[]{
      null,
      null,
      null,
      "3.5.7",
      "3.5.7",
      "3.5.7",
      "20101201",
      "20101201",
      "20101201",
      "3.0.5.RELEASE",
      "3.0.5.RELEASE",
      "3.0.5.RELEASE",
      "2.0.0-wip-dev",
      "2.0.0-wip-dev",
      "2.0.0-wip-dev",
      "2.0.0-wip-dev"
    };

    for( int i = 0; i < paths.length; i++ )
      {
      assertEquals( paths[ i ], names[ i ], Util.findName( paths[ i ] ) );
      assertEquals( paths[ i ], versions[ i ], Util.findVersion( paths[ i ] ) );
      }
    }

  @Test
  public void testContainsWhiteSpace()
    {
    assertFalse( Util.containsWhitespace( "" ) );
    assertFalse( Util.containsWhitespace( "aGoodTagMayLookLikeThis" ) );
    assertTrue( Util.containsWhitespace( " aaa" ) );
    assertTrue( Util.containsWhitespace( "aaa\t " ) );
    assertTrue( Util.containsWhitespace( "\tabc\tdef\tghi\t" ) );
    assertTrue( Util.containsWhitespace( "contains\tstuff\twe\rdon't\nwant\f" ) );
    }

  String[] results = new String[]{
    null,
    null,
    "www.example.com",
    "ip-172-31-11-27.us-west-2.compute.internal",
    "172.31.11.27",
    "www.example.com",
    "hadoop42.example.com",
    "hadoop42.example.com",
    "hadoop42.example.com",
    "hadoop42.example.com",
    "hadoop42.example.com",
    "hadoop42.example.com",
    "some-bucket",
    "some-bucket"
  };

  String[] uris = new String[]{
    null,
    "",
    "www.example.com",
    "http://ip-172-31-11-27.us-west-2.compute.internal:20888/proxy/application_1451945425832_15086/",
    "http://172.31.11.27:20888/proxy/application_1451945425832_15086/",
    "http://www.example.com:8080/docs/resource1.html?user=foo&password=secret&action=do",
    "hdfs://hadoop42.example.com:8020/some/dataset",
    "hdfs://foor:bar@hadoop42.example.com:8020/some/dataset",
    "hdfs://foor:bar@hadoop42.example.com:8020/some/dataset#@foo/:baz",
    "hadoop42.example.com:8020/some/dataset", // hadoop leaves out the uri sometimes
    "foor:bar@hadoop42.example.com:8020/some/dataset",
    "foor:bar@hadoop42.example.com/some/dataset",
    "s3n://some-bucket/2014/12/2[5-9]/*",
     "s3a://some-bucket/2014/12/2[5-9]/*"
  };

  @Test
  public void testURIHostnameParser() throws Exception
    {
    for( int i = 0; i < results.length; i++ )
      {
      String result = results[ i ];
      String uri = uris[ i ];

      assertEquals( result, Util.parseHostname( uri ) );
      }
    }
  }
