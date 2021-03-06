/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.intel.oap.fs.hadoop.cachedfs;

import org.apache.hadoop.fs.Path;

public class CachedFileSystemUtils {

    public static PMemBlock[] computePossiblePMemBlocks(Path path,
                                                        long start,
                                                        long len,
                                                        long blockSize) {
        PMemBlock[] ret = new PMemBlock[0];

        if (path == null || start < 0 || len <= 0 || blockSize <= 0) {
            return ret;
        }

        long blkStart = start - (start % blockSize);
        long blkEnd = ((start + len) % blockSize == 0) ? start + len
                : start + len - ((start + len) % blockSize) + blockSize;
        long blkNum = (blkEnd - blkStart) / blockSize;

        ret = new PMemBlock[(int)blkNum];

        for (int i = 0; i < blkNum; i++) {
            ret[i] = new PMemBlock(path, blkStart + (i * blockSize), blockSize);
        }

        return ret;
    }
}
