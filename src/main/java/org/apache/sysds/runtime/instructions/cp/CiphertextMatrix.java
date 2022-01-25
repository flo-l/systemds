package org.apache.sysds.runtime.instructions.cp;

import org.apache.sysds.common.Types;
import org.apache.sysds.runtime.meta.DataCharacteristics;

/**
 * This class abstracts over an encrypted matrix of ciphertexts. It stores the data as opaque byte array. The layout is unspecified.
 */
public class CiphertextMatrix extends Encrypted {
    private static final long serialVersionUID = 1762936872261940616L;

    public CiphertextMatrix(int[] dims, DataCharacteristics dc, byte[] data) {
        super(dims, dc, data, Types.DataType.ENCRYPTED_CIPHER);
    }

    @Override
    public String getDebugName() {
        return "CiphertextMatrix " + getData().hashCode();
    }
}
