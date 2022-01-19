package org.apache.sysds.runtime.controlprogram.paramserv.homomorphicEncryption;

import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.instructions.cp.CiphertextMatrix;
import org.apache.sysds.runtime.instructions.cp.PlaintextMatrix;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.utils.NativeHelper;

import java.util.stream.IntStream;

public class SEALClient {
    public SEALClient(byte[] a) {
        // TODO take params here, like slot_count etc.
        // TODO init ctx and block_size
        ctx = NativeHelper.initClient(a);
    }

    // this is a pointer to the context used by all native methods of this class
    private final long ctx;


    // generates a partial public key and returns it
    // stores a partial private key corresponding to the partial public key in ctx
    public PublicKey generatePartialPublicKey() {
        return new PublicKey(NativeHelper.generatePartialPublicKey(ctx));
    }

    // sets the public key and stores it in ctx
    public void setPublicKey(PublicKey public_key) {
        NativeHelper.setPublicKey(ctx, public_key.getData());
    }

    // encrypts one block of data with public key stored statically and returns it
    // setPublicKey() must have been called before calling this
    // half_block is half the size of SEAL slot_count
    public CiphertextMatrix encrypt(MatrixObject plaintext) {
        MatrixBlock mb = plaintext.acquireReadAndRelease();
        int[] dims = IntStream.range(0, 4).map(mb.getDenseBlock()::getDim).toArray();
        double[] raw_data = mb.getDenseBlockValues();
        return new CiphertextMatrix(dims, plaintext.getDataCharacteristics(), NativeHelper.encrypt(ctx, raw_data));
    }

    // partially decrypts one block with the partial private key. generatePartialPublicKey() must
    // have been called before calling this function
    //  returns a block half the size of SEAL slot_count
    public PlaintextMatrix partiallyDecrypt(CiphertextMatrix ciphertext) {
        return new PlaintextMatrix(ciphertext.getDims(), ciphertext.getDataCharacteristics(), NativeHelper.partiallyDecrypt(ctx, ciphertext.getData()));
    }
}
