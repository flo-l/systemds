package org.apache.sysds.runtime.controlprogram.paramserv;

import org.apache.sysds.parser.Statement;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.instructions.cp.ListObject;

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
    private synchronized <T> T collectAndDo(int workerId, T obj, Function<List<T>, T> f) {
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

        return (T)result;
    }

    private ListObject sum(List<ListObject> summands) {
        ListObject sum = summands.get(0);
        for (int i = 1; i < summands.size(); i++) {
            ParamservUtils.accrueGradients(sum, summands.get(i), true);
        }
        return sum;
    }

    private ListObject homomorphicAggregation(List<ListObject> encrypted_models) {
        // TODO: implement with SEAL
        return sum(encrypted_models);
    }

    private ListObject homomorphicDecryption(List<ListObject> partial_decryptions) {
        // TODO: implement with SEAL
        return sum(partial_decryptions);
    }

    @Override
    public void push(int workerID, ListObject encrypted_model) {
        // wait for all updates and sum them homomorphically
        ListObject homomorphic_sum = collectAndDo(workerID, encrypted_model, this::homomorphicAggregation);

        // get partial decryptions
        ListObject partial_decryption = _threads.get(workerID).getPartialDecryption(homomorphic_sum);

        ListObject new_model = collectAndDo(workerID, partial_decryption, this::homomorphicDecryption);
        // FIXME: this is called with the same data from every thread. we could also just call it once and skip the averaging step. anyhow, this is easier to implement for the POC.
        updateGlobalModel(workerID, new_model);
    }
}
