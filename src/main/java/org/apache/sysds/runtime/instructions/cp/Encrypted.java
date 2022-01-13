package org.apache.sysds.runtime.instructions.cp;

import org.apache.sysds.common.Types;

/**
 * This class abstracts over an encrypted data. It stores the data as opaque byte array. The layout is unspecified.
 */
public abstract class Encrypted extends Data {
    private static final long serialVersionUID = 1762936872268046168L;

    private final byte[] _data;

    public Encrypted(byte[] data) {
        super(Types.DataType.UNKNOWN, Types.ValueType.UNKNOWN);
        _data = data;
    }

    public byte[] getData() {
        return _data;
    }
}
