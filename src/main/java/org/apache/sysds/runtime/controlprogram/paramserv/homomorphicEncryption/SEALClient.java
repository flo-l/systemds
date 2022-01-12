package org.apache.sysds.runtime.controlprogram.paramserv.homomorphicEncryption;

public class SEALClient {
    static {
        System.load("/var/home/me/Dokumente/uni/01_master/masterarbeit/systemds/src/main/java/org/apache/sysds/runtime/controlprogram/paramserv/homomorphicEncryption/cpp/target/libseal_implementation.so");
    }

    public SEALClient() {
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

    // generates a partial public key and returns it
    // stores a partial private key corresponding to the partial public key in ctx
    public native long[] generatePartialPublicKey();

    // sets the public key and stores it in ctx
    public native void setPublicKey(long[] public_key);

    // encrypts one block of data with public key stored statically and returns it
    // setPublicKey() must have been called before calling this
    // half_block is half the size of SEAL slot_count
    public native long[] encryptBlock(double[] plaintext);

    // partially decrypts one block with the partial private key. generatePartialPublicKey() must
    // have been called before calling this function
    //  returns a block half the size of SEAL slot_count
    public native double[] partiallyDecryptBlock(long[] ciphertext);

}
