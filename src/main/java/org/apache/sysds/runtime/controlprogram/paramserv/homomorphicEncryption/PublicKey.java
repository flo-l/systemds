package org.apache.sysds.runtime.controlprogram.paramserv.homomorphicEncryption;

import java.io.Serializable;

public class PublicKey implements Serializable {
    private static final long serialVersionUID = 91289081237980123L;

    private final byte[] _data;

    public PublicKey(byte[] data) {
        _data = data;
    }

    public byte[] getData() {
        return _data;
    }
}
