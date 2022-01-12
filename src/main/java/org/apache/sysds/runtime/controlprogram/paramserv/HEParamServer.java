package org.apache.sysds.runtime.controlprogram.paramserv;

import org.apache.sysds.hops.OptimizerUtils;
import org.apache.sysds.parser.Statement;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.controlprogram.paramserv.homomorphicEncryption.SEALServer;
import org.apache.sysds.runtime.data.DenseBlock;
import org.apache.sysds.runtime.data.DenseBlockFP64;
import org.apache.sysds.runtime.data.DenseBlockInt64;
import org.apache.sysds.runtime.functionobjects.Plus;
import org.apache.sysds.runtime.instructions.cp.Data;
import org.apache.sysds.runtime.instructions.cp.ListObject;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.operators.BinaryOperator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.DoubleSupplier;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/// This class implements Homomorphic Encryption (HE) for LocalParamServer. It only supports modelAvg=true.
public class HEParamServer extends LocalParamServer {
    private int _thread_counter = 0;
    private final List<FederatedPSControlThread> _threads;
    private final List<Object> _result_buffer; // one per thread
    private Object result;
    private final SEALServer _seal_server;

    public static HEParamServer create(ListObject model, String aggFunc, Statement.PSUpdateType updateType,
                                          Statement.PSFrequency freq, ExecutionContext ec, int workerNum, String valFunc, int numBatchesPerEpoch,
                                          MatrixObject valFeatures, MatrixObject valLabels, int nbatches)
    {
        return new HEParamServer(model, aggFunc, updateType, freq, ec,
                workerNum, valFunc, numBatchesPerEpoch, valFeatures, valLabels, nbatches);
    }

    private HEParamServer(ListObject model, String aggFunc, Statement.PSUpdateType updateType,
                             Statement.PSFrequency freq, ExecutionContext ec, int workerNum, String valFunc, int numBatchesPerEpoch,
                             MatrixObject valFeatures, MatrixObject valLabels, int nbatches)
    {
        super(model, aggFunc, updateType, freq, ec, workerNum, valFunc, numBatchesPerEpoch, valFeatures, valLabels, nbatches, true);

        _seal_server = new SEALServer();

        _threads = Collections.synchronizedList(new ArrayList<>(workerNum));
        for (int i = 0; i < getNumWorkers(); i++) {
            _threads.add(null);
        }

        _result_buffer = new ArrayList<>(workerNum);
        resetResultBuffer();
    }

    public void registerThread(int thread_id, FederatedPSControlThread thread) {
        _threads.set(thread_id, thread);
    }

    private synchronized void resetResultBuffer() {
        _result_buffer.clear();
        for (int i = 0; i < getNumWorkers(); i++) {
            _result_buffer.add(null);
        }
    }

    // this method collects all T Objects from each worker into a list and then calls f once on this list to produce
    // another T, which it returns.
    private synchronized <T,U> U collectAndDo(int workerId, T obj, Function<List<T>, U> f) {
        _result_buffer.set(workerId, obj);
        _thread_counter++;

        if (_thread_counter == getNumWorkers()) {
            List<T> buf = _result_buffer.stream().map(x -> (T)x).collect(Collectors.toList());
            result = f.apply(buf);
            resetResultBuffer();
            _thread_counter = 0;
            notifyAll();
        } else {
            try {
                wait();
            } catch (InterruptedException i) {
                throw new RuntimeException("thread interrupted");
            }
        }

        return (U)result;
    }

    private ListObject sum(List<ListObject> summands) {
        ListObject sum = summands.get(0);
        for (int i = 1; i < summands.size(); i++) {
            ParamservUtils.accrueGradients(sum, summands.get(i), true);
        }
        return sum;
    }

    private long[][] homomorphicAggregation(List<ListObject> encrypted_models) {
        // TODO: implement with SEAL
        //  for each model m
        //    for each matrixObject in m
        //      for each block of size SEALServer::blocksize
        //              extract data
        //              call SEAL
        //              store result

        long[][] seal_ary = new long[encrypted_models.size()][];
        long[][] result = new long[encrypted_models.get(0).getLength()][];
        final int seal_block_size = _seal_server.getBlockSize();
        for (int matrix_idx = 0; matrix_idx < encrypted_models.get(0).getLength(); matrix_idx++) {
            DenseBlock first = ((MatrixObject)encrypted_models.get(0).getData(0)).acquireReadAndRelease().getDenseBlock();
            result[matrix_idx] = new long[first.getCumODims(0)*first.getDim(0)];
            for (int block_idx = 0; block_idx < first.numBlocks(); block_idx++) {
                for (int model_idx = 0; model_idx < encrypted_models.size(); model_idx++) {
                    DenseBlock block = ((MatrixObject)encrypted_models.get(model_idx).getData(matrix_idx)).acquireReadAndRelease().getDenseBlock();
                    DoubleStream s = DoubleStream.of(block.valuesAt(block_idx));
                    DoubleStream t = DoubleStream.of(0);
                    long[] seal_chunk = DoubleStream.concat(s, t)
                            .skip(seal_block_size * block_idx)
                            .limit(_seal_server.getBlockSize())
                            .mapToLong(Math::round)
                            .toArray();
                    seal_ary[model_idx] = seal_chunk;
                }
                long[] res_data = _seal_server.accumulateCiphertexts(seal_ary);
                System.arraycopy(res_data, 0, result[matrix_idx], seal_block_size * block_idx, seal_block_size);
            }
        }
        return result;
    }

    private long[] homomorphicDecryption(List<long[][]> partial_decryptions) {
        // TODO: implement with SEAL
        //  for partial_decryption model p
        //      for each matrixObject in p
        //          for each block of size paramserv::blocksize
        //              extract data
        //              call SEAL
        //              store result

        double[] decryption = new double[]{}; // TODO: calc with SEAL
        int[] dims = new int[]{}; // TODO get from args
        DenseBlock db = new DenseBlockFP64(dims);
        int[] curr_index = new int[]{0, 0, 0, 0};
        for (int i = 0; i < db.size(); i++) {
            db.set(curr_index, decryption[i]);
            db.getNextIndexes(curr_index);
        }
        MatrixBlock mb = new MatrixBlock(db.numRows(), db.getDim(1), db);
        MatrixObject mo = new MatrixObject(mb);

        return null;
    }

    @Override
    public void push(int workerID, ListObject encrypted_model) {
        // wait for all updates and sum them homomorphically
        long[][] homomorphic_sum = collectAndDo(workerID, encrypted_model, this::homomorphicAggregation);

        // get partial decryptions
        long[][] partial_decryption = _threads.get(workerID).getPartialDecryption(homomorphic_sum);

        long[] new_model_data = collectAndDo(workerID, partial_decryption, this::homomorphicDecryption);

        ListObject old_model = getResult();
        ListObject new_model = new ListObject(old_model);

        for (int i = 0; i < new_model.getLength(); i++) {
            MatrixObject old_obj = (MatrixObject) new_model.getData(i);
            MatrixObject new_obj = ExecutionContext.createMatrixObject(old_obj.getMetaData().getDataCharacteristics());

            int[] dims = old_obj.acquireReadAndRelease().getDenseBlock(); // TODO get from args
            DenseBlock db = new DenseBlockFP64(dims);
            int[] curr_index = new int[]{0, 0, 0, 0};
            for (int i = 0; i < db.size(); i++) {
                db.set(curr_index, decryption[i]);
                db.getNextIndexes(curr_index);
            }
            MatrixBlock mb = new MatrixBlock(db.numRows(), db.getDim(1), db);
            MatrixObject mo = new MatrixObject(mb);


            new_obj.acquireModify(new MatrixBlock());
            new_obj.acquireModify();

        }


        ExecutionContext

        OptimizerUtils.getUniqueTempFileName

        // FIXME: this is called with the same data from every thread. we could also just call it once and skip the averaging step. anyhow, this is easier to implement for the POC.
        updateGlobalModel(workerID, new_model);
    }
}
