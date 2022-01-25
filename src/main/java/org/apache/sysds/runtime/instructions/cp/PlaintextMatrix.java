package org.apache.sysds.runtime.instructions.cp;

import org.apache.sysds.common.Types;
import org.apache.sysds.runtime.meta.DataCharacteristics;

/**
 * This class abstracts over an encrypted matrix of ciphertexts. It stores the data as opaque byte array. The layout is unspecified.
 */
public class PlaintextMatrix extends Encrypted {
    private static final long serialVersionUID = 5732436872261940616L;

    public PlaintextMatrix(int[] dims, DataCharacteristics dc, byte[] data) {
        super(dims, dc, data, Types.DataType.ENCRYPTED_PLAIN);
    }

    @Override
    public String getDebugName() {
        return "PlaintextMatrix " + getData().hashCode();
    }
}
