package com.data.bt.schemaupgrade.celldecoder;

import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.kiji.schema.impl.AvroCellDecoder;
import org.kiji.schema.layout.CellSpec;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: acohen
 * Date: 10/9/13
 * Time: 11:21 AM
 * To change this template use File | Settings | File Templates.
 */
public class SpecificCellDecoderWithOnlyReaderSchema<T> extends AvroCellDecoder<T> {

    /**
     * Initializes a cell decoder that creates specific Avro types.
     *
     * @param cellSpec Specification of the cell encoding.
     * @throws java.io.IOException on I/O error.
     */
    public SpecificCellDecoderWithOnlyReaderSchema(CellSpec cellSpec) throws IOException {
        super(cellSpec);
    }

    /** {@inheritDoc} */
    @Override
    protected DatumReader<T> createDatumReader(Schema writer, Schema reader) {
        return new SpecificDatumReader<T>(reader, reader);
    }
}
