package org.apache.sysds.runtime.instructions.cp;

/**
 * This class abstracts over an encrypted matrix of ciphertexts. It stores the data as opaque byte array. The layout is unspecified.
 */
public class PlaintextMatrix extends Encrypted {
    private static final long serialVersionUID = 5732436872261940616L;

    public PlaintextMatrix(byte[] data) {
        super(data);
    }

    @Override
    public String getDebugName() {
        return "PlaintextMatrix " + getData().hashCode();
    }
}
