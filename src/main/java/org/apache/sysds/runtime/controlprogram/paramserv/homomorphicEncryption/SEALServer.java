package org.apache.sysds.runtime.controlprogram.paramserv.homomorphicEncryption;

public class SEALServer {
    static {
        System.load("/var/home/me/Dokumente/uni/01_master/masterarbeit/systemds/src/main/java/org/apache/sysds/runtime/controlprogram/paramserv/homomorphicEncryption/cpp/target/libseal_implementation.so");
    }

    public SEALServer() {
        // TODO take params here, like slot_count etc.
        // TODO init ctx and block_size
    }

    public int getBlockSize() {
        return _block_size;
    }
    private int _block_size = -1;

    // TODO: use this to init ctx
    private native void init();

    // this is a pointer to the context used by all native methods of this class
    private long ctx = 0;

    // NOTICE: all long[] arys here have to be of size SEAL slot_count
    // they represent the data of one Ciphertext object
    // NOTICE: all double[] arys here have to be half the size of SEAL slot_count
    // they represent the data of one Plaintext object

    // accumulates the given partial public keys into a public key, stores it in ctx and returns it
    public native long[] accumulatePartialPublicKeys(long[][] partial_public_keys);

    // accumulates the given ciphertext blocks into a sum ciphertext and returns it
    // stores c0 of the sum to be used in averageBlocks()
    public native long[] accumulateCiphertexts(long[][] ciphertexts);

    // averages the partial decryptions, which are all half the size of SEAL slot_count and returns the result as
    // half_block and
    // encrypted_sum is the result of accumulateCiphertexts() and partial_plaintexts is the result of partiallyDecryptBlock
    // of each ciphertext fed into accumulateCiphertexts
    public native double[] averageBlocks(long[] encrypted_sum, double[][] partial_plaintexts);
}