package org.apache.sysds.runtime.controlprogram.paramserv.homomorphicEncryption;

import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.data.DenseBlock;

public class MatrixObjectChunkReader {
    private final MatrixObject _mob;
    private final int _chunk_size;

    private boolean _done = false;

    // current block for _next_chunk
    private int _current_block = 0;
    // offset in current block for _next_chunk
    private int _current_block_offset = 0;
    // so the first valid element in the next chunk is blocks[_current_block][_current_block_offset]

    public MatrixObjectChunkReader(MatrixObject mob, int chunk_size) {
        assert !mob.acquireReadAndRelease().isInSparseFormat() : "can't handle sparse matrix blocks";
        assert chunk_size > 0 : "invalid chunk size";
        _mob = mob;
        _chunk_size = chunk_size;
    }

    // writes a chunk of _chunk_size into buf
    // returns the number of doubles written
    public int getNextChunk(double[] buf) {
        assert buf.length == _chunk_size : "wrong size of buf";

        // check if we are done iterating
        if (_done) {
            return 0;
        }

        DenseBlock db = _mob.acquireReadAndRelease().getDenseBlock();
        double[] result = new double[_chunk_size];

        int already_copied = 0;

        while (already_copied < _chunk_size && !_done) {
            int copy_from_current_block = Math.min(_chunk_size - already_copied, db.valuesAt(_current_block).length - _current_block_offset);
            System.arraycopy(db.valuesAt(_current_block), _current_block_offset, result, already_copied, copy_from_current_block);
            already_copied += copy_from_current_block;
            _current_block_offset += copy_from_current_block;

            // advance indices
            if (_current_block_offset >= db.valuesAt(_current_block).length) {
                _current_block++;
                _current_block_offset = 0;
                // check if we're done
                if (_current_block >= db.numBlocks()) {
                    _done = true;
                }
            }
        }
        return already_copied;
    }
}
