package org.apache.sysds.runtime.controlprogram.paramserv.homomorphicEncryption;

import org.apache.spark.sql.sources.In;
import org.apache.sysds.common.Types;
import org.apache.sysds.runtime.controlprogram.caching.CacheableData;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.data.DenseBlockInt64;
import org.apache.sysds.runtime.instructions.cp.Data;
import org.apache.sysds.runtime.instructions.cp.IntObject;
import org.apache.sysds.runtime.instructions.cp.ListObject;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.runtime.meta.MatrixCharacteristics;
import org.apache.sysds.runtime.meta.MetaData;

import java.io.Serializable;

public class PublicKey implements Serializable {
    private static final long serialVersionUID = 91289081237980123L;

    public Data toData() {
        int size = 1;
        /*
        MatrixBlock mb = new MatrixBlock();
        double[] ary = new double[size];
        for (int i = 0; i < size; i++) {
            // TODO: copy data
            ary[i] = 0;
        }

        mb.init(ary, 1, size);
        return new MatrixObject(Types.ValueType.FP64, "", new MetaData(new MatrixCharacteristics(1, size)), mb);
        */

        // TODO: maybe find a better representation of this (Matrix? [bad bc it needs a file], a new Data subclass?)
        // the right data type is MatrixBlock or even better long[]
        IntObject[] data_ary = new IntObject[size];
        for (int i = 0; i < size; i++) {
            // TODO: copy data
            data_ary[i] = new IntObject(42);
        }
        return new ListObject(data_ary);
    }
}
