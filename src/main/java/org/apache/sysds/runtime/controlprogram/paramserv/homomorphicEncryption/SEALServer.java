package org.apache.sysds.runtime.controlprogram.paramserv.homomorphicEncryption;

import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.instructions.cp.CiphertextMatrix;
import org.apache.sysds.runtime.instructions.cp.PlaintextMatrix;

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

    // this generates the a constant. in a future version we want to generate this together with the clients to prevent misuse
    public native PublicKey generateA();

    // accumulates the given partial public keys into a public key, stores it in ctx and returns it
    public native PublicKey aggregatePartialPublicKeys(PublicKey[] partial_public_keys);

    // accumulates the given ciphertext blocks into a sum ciphertext and returns it
    // stores c0 of the sum to be used in averageBlocks()
    public native CiphertextMatrix accumulateCiphertexts(CiphertextMatrix[] ciphertexts);

    // averages the partial decryptions, which are all half the size of SEAL slot_count and returns the result as
    // half_block and
    // encrypted_sum is the result of accumulateCiphertexts() and partial_plaintexts is the result of partiallyDecryptBlock
    // of each ciphertext fed into accumulateCiphertexts
    public native MatrixObject average(CiphertextMatrix encrypted_sum, PlaintextMatrix[] partial_plaintexts);

    /*

        private static long[] extractData(Data matrix_object) {
        MatrixObject mobj = (MatrixObject) matrix_object;
        return DataConverter.toLong(mobj.acquireReadAndRelease().getDenseBlockValues());
    }

        ListObject old_model = getResult();
        ListObject new_model = new ListObject(old_model);

        for (int i = 0; i < new_model.getLength(); i++) {
            MatrixObject old_obj = (MatrixObject) new_model.getData(i);
            MatrixBlock old_matrix_block = old_obj.acquireReadAndRelease();
            DenseBlock old_dense_block = old_matrix_block.getDenseBlock();
            int[] dims = IntStream.range(0, 4).map(old_dense_block::getDim).toArray();

            DenseBlock new_dense_block = DenseBlockFactory.createDenseBlock(new_model_data[i], dims);
            MatrixBlock new_matrix_block = new MatrixBlock(old_matrix_block.getNumRows(), old_matrix_block.getNumColumns(), new_dense_block);
            MatrixObject new_obj = ExecutionContext.createMatrixObject(old_obj.getMetaData().getDataCharacteristics());
            new_obj.acquireModify(new_matrix_block);
            new_model.set(i, new_obj);
        }
     */
}