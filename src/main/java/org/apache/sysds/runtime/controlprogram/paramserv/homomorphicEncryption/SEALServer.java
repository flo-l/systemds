package org.apache.sysds.runtime.controlprogram.paramserv.homomorphicEncryption;

import org.apache.sysds.common.Types;
import org.apache.sysds.conf.ConfigurationManager;
import org.apache.sysds.hops.OptimizerUtils;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.data.DenseBlock;
import org.apache.sysds.runtime.data.DenseBlockFactory;
import org.apache.sysds.runtime.instructions.cp.CiphertextMatrix;
import org.apache.sysds.runtime.instructions.cp.Encrypted;
import org.apache.sysds.runtime.instructions.cp.PlaintextMatrix;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.runtime.meta.MatrixCharacteristics;
import org.apache.sysds.runtime.meta.MetaDataFormat;
import org.apache.sysds.utils.NativeHelper;

import java.nio.DoubleBuffer;
import java.util.Arrays;

public class SEALServer {
    public SEALServer() {
        // TODO take params here, like slot_count etc.
        ctx = NativeHelper.initServer();
    }

    // this is a pointer to the context used by all native methods of this class
    private final long ctx;
    private byte[] _a;

    // NOTICE: all long[] arys here have to be of size SEAL slot_count
    // they represent the data of one Ciphertext object
    // NOTICE: all double[] arys here have to be half the size of SEAL slot_count
    // they represent the data of one Plaintext object

    // this generates the a constant. in a future version we want to generate this together with the clients to prevent misuse
    public synchronized byte[] generateA() {
        if (_a == null) {
            _a = NativeHelper.generateA(ctx);
        }
        return _a;
    }

    // accumulates the given partial public keys into a public key, stores it in ctx and returns it
    public PublicKey aggregatePartialPublicKeys(PublicKey[] partial_public_keys) {
        return new PublicKey(NativeHelper.aggregatePartialPublicKeys(ctx, extractRawData(partial_public_keys)));
    }

    // accumulates the given ciphertext blocks into a sum ciphertext and returns it
    // stores c0 of the sum to be used in averageBlocks()
    public CiphertextMatrix accumulateCiphertexts(CiphertextMatrix[] ciphertexts) {
        return new CiphertextMatrix(ciphertexts[0].getDims(), ciphertexts[0].getDataCharacteristics(), NativeHelper.accumulateCiphertexts(ctx, extractRawData(ciphertexts)));
    }

    // averages the partial decryptions and stores the result in old_mo
    // encrypted_sum is the result of accumulateCiphertexts() and partial_plaintexts is the result of partiallyDecryptBlock
    // of each ciphertext fed into accumulateCiphertexts
    public MatrixObject average(CiphertextMatrix encrypted_sum, PlaintextMatrix[] partial_plaintexts) {
        double[] raw_result = NativeHelper.average(ctx, encrypted_sum.getData(), extractRawData(partial_plaintexts));
        int[] dims = encrypted_sum.getDims();
        int result_len = Arrays.stream(dims).reduce(1, (x,y) -> x*y);
        DataCharacteristics dc = encrypted_sum.getDataCharacteristics();

        DenseBlock new_dense_block = DenseBlockFactory.createDenseBlock(Arrays.copyOf(raw_result, result_len), dims);
        MatrixBlock new_matrix_block = new MatrixBlock((int)dc.getRows(), (int)dc.getCols(), new_dense_block);
        MatrixObject new_mo = new MatrixObject(Types.ValueType.FP64, OptimizerUtils.getUniqueTempFileName(), new MetaDataFormat(dc, Types.FileFormat.BINARY), new_matrix_block);
        new_mo.exportData(); // write data, otherwise it might get evicted and thus get lost
        return new_mo;
    }

    private static byte[][] extractRawData(Encrypted[] data) {
        byte[][] raw_data = new byte[data.length][];
        for (int i = 0; i < data.length; i++) {
            raw_data[i] = data[i].getData();
        }
        return raw_data;
    }

    // TODO: extract an interface for this and use it here
    private static byte[][] extractRawData(PublicKey[] data) {
        byte[][] raw_data = new byte[data.length][];
        for (int i = 0; i < data.length; i++) {
            raw_data[i] = data[i].getData();
        }
        return raw_data;
    }

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