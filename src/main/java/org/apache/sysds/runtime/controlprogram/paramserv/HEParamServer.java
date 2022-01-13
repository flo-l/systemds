package org.apache.sysds.runtime.controlprogram.paramserv;

import org.apache.sysds.parser.Statement;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.controlprogram.paramserv.homomorphicEncryption.PublicKey;
import org.apache.sysds.runtime.controlprogram.paramserv.homomorphicEncryption.SEALServer;
import org.apache.sysds.runtime.instructions.cp.CiphertextMatrix;
import org.apache.sysds.runtime.instructions.cp.ListObject;
import org.apache.sysds.runtime.instructions.cp.PlaintextMatrix;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

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

    public PublicKey aggregatePartialPublicKeys(PublicKey[] partial_public_keys) {
        return _seal_server.aggregatePartialPublicKeys(partial_public_keys);
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

    private CiphertextMatrix[] homomorphicAggregation(List<ListObject> encrypted_models) {
        CiphertextMatrix[] result = new CiphertextMatrix[encrypted_models.get(0).getLength()];
        for (int matrix_idx = 0; matrix_idx < encrypted_models.get(0).getLength(); matrix_idx++) {
            CiphertextMatrix[] summands = new CiphertextMatrix[encrypted_models.size()];
            for (int i = 0; i < encrypted_models.size(); i++) {
                summands[i] = (CiphertextMatrix) encrypted_models.get(i).getData(matrix_idx);
            }
            result[matrix_idx] = _seal_server.accumulateCiphertexts(summands);;
        }
        return result;
    }

    private MatrixObject[] homomorphicAverage(List<PlaintextMatrix[]> partial_decryptions) {
        // TODO: implement with SEAL
        //  for partial_decryption model p
        //      for each matrixObject in p
        //          for each block of size paramserv::blocksize
        //              extract data
        //              call SEAL
        //              store result
        MatrixObject[] result = new MatrixObject[partial_decryptions.get(0).length];
        for (int matrix_idx = 0; matrix_idx < partial_decryptions.get(0).length; matrix_idx++) {
            PlaintextMatrix[] partial_plaintexts = new PlaintextMatrix[partial_decryptions.size()];
            for (int i = 0; i < partial_decryptions.size(); i++) {
                partial_plaintexts[i] = partial_decryptions.get(i)[matrix_idx];
            }

            result[matrix_idx] = _seal_server.average(null, partial_plaintexts);
        }
        return result;
    }

    @Override
    public void push(int workerID, ListObject encrypted_model) {
        // wait for all updates and sum them homomorphically
        CiphertextMatrix[] homomorphic_sum = collectAndDo(workerID, encrypted_model, this::homomorphicAggregation);

        // get partial decryptions
        PlaintextMatrix[] partial_decryption = _threads.get(workerID).getPartialDecryption(homomorphic_sum);

        MatrixObject[] new_model_matrices = collectAndDo(workerID, partial_decryption, this::homomorphicAverage);

        ListObject old_model = getResult();
        ListObject new_model = new ListObject(old_model);
        for (int i = 0; i < new_model.getLength(); i++) {
            new_model.set(i, new_model_matrices[i]);
        }

        // FIXME: this is called with the same data from every thread. we could also just call it once and skip the averaging step. anyhow, this is easier to implement for the POC.
        updateGlobalModel(workerID, new_model);
    }
}
