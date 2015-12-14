package com.ociweb.protocoltest.data;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;

public class PhastDecoder {

    static long decodeDeltaLong(long[] longDictionary, DataInputBlobReader reader, long map, int idx, long defaultValue, int bitMask) {
        return (0==(map&bitMask)) ? (longDictionary[idx] += reader.readPackedLong()) : defaultValue;        
    }

    static int decodeDefaultInt(DataInputBlobReader reader, long map, int defaultValue, int bitMask) {
       return (0==(map&bitMask)) ? defaultValue : reader.readPackedInt();
    }

    static int decodeDeltaInt(int[] intDictionary, DataInputBlobReader reader, long map, int idx, int defaultValue, int bitMask) {
        return (0==(map&bitMask)) ? (intDictionary[idx] += reader.readPackedInt()) : defaultValue;
    }

    static int decodeIncInt(int[] intDictionary, DataInputBlobReader reader, long map, int idx, int bitMask) {
        if (0!=(map&bitMask)) {
            intDictionary[idx] = reader.readPackedInt();
        }
        return intDictionary[idx]++;
    }

}
