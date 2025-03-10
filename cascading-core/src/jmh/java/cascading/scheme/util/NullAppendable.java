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

package cascading.scheme.util;

import java.io.IOException;

/**
 *
 */
class NullAppendable implements Appendable
  {
  @Override
  public Appendable append( CharSequence csq ) throws IOException
    {
    return this;
    }

  @Override
  public Appendable append( CharSequence csq, int start, int end ) throws IOException
    {
    return this;
    }

  @Override
  public Appendable append( char c ) throws IOException
    {
    return this;
    }
  }
