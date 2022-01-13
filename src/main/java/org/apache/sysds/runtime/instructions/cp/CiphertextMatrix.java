package org.apache.sysds.runtime.instructions.cp;

/**
 * This class abstracts over an encrypted matrix of ciphertexts. It stores the data as opaque byte array. The layout is unspecified.
 */
public class CiphertextMatrix extends Encrypted {
    private static final long serialVersionUID = 1762936872261940616L;

    public CiphertextMatrix(byte[] data) {
        super(data);
    }

    @Override
    public String getDebugName() {
        return "CiphertextMatrix " + getData().hashCode();
    }
}
