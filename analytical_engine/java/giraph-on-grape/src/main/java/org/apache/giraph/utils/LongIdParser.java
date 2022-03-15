/*
 * Copyright 2021 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.giraph.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LongIdParser {

    private static Logger logger = LoggerFactory.getLogger(LongIdParser.class);
    private int fidOffset;
    private int labelIdOffset;
    private long fidMask;
    private long lidMask;
    private long labelIdMask;
    private long offsetMask;

    public LongIdParser(int fnum, int labelNum) {
        if (labelNum >= 128) {
            throw new IllegalStateException("max label num: 128, cur: " + labelNum);
        }
        int fidWidth = numToBitWidth(fnum);
        fidOffset = 64 - fidWidth;
        int labelWidth = numToBitWidth(128);
        labelIdOffset = fidOffset - labelWidth;
        fidMask = ((1L << fidWidth) - 1L) << fidOffset;
        lidMask = (1L << fidOffset) - 1L;
        labelIdMask = ((1L << labelWidth) - 1L) << labelIdOffset;
        offsetMask = (1L << labelIdOffset) - 1L;
        logger.info(
                "fid offset [{}], labelIdOffset [{}], fidMast [{}], lidMask [{}], labelIdMask [{}],"
                        + " offsetMask[{}]",
                fidOffset,
                labelIdOffset,
                fidMask,
                lidMask,
                labelIdMask,
                offsetMask);
    }

    public int getFid(long v) {
        return (int) (v >> fidOffset);
    }

    public int getLabelId(long v) {
        return (int) ((v & labelIdMask) >> labelIdOffset);
    }

    public long getOffset(long v) {
        return (v & offsetMask);
    }

    //    ID_TYPE GetLid(ID_TYPE v) const { return v & lid_mask_; }
    //
    //    ID_TYPE GenerateId(fid_t fid, LabelIDT label, int64_t offset) const {
    //        return (((ID_TYPE) offset) & offset_mask_) |
    //            ((((ID_TYPE) label) << label_id_offset_) & label_id_mask_) |
    //            ((((ID_TYPE) fid) << fid_offset_) & fid_mask_);
    //    }

    public long offset_mask() {
        return offsetMask;
    }

    private int numToBitWidth(int num) {
        if (num <= 2) {
            return 1;
        }
        int max = num - 1;
        int width = 0;
        while (max > 0) {
            ++width;
            max >>= 1;
        }
        return width;
    }
}
