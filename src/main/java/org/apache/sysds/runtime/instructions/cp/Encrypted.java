package org.apache.sysds.runtime.instructions.cp;

import org.apache.sysds.common.Types;
import org.apache.sysds.runtime.meta.DataCharacteristics;

/**
 * This class abstracts over an encrypted data. It stores the data as opaque byte array. The layout is unspecified.
 */
public abstract class Encrypted extends Data {
    private static final long serialVersionUID = 1762936872268046168L;

    private final int[] _dims;
    private final DataCharacteristics _dc;
    private final byte[] _data;

    public Encrypted(int[] dims, DataCharacteristics dc, byte[] data, Types.DataType dt) {
        super(dt, Types.ValueType.UNKNOWN);
        _dims = dims;
        _dc = dc;
        _data = data;
    }

    public int[] getDims() {
        return _dims;
    }

    public DataCharacteristics getDataCharacteristics() {
        return _dc;
    }

    public byte[] getData() {
        return _data;
    }
}
