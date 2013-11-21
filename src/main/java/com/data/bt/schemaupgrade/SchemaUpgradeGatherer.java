package com.data.bt.schemaupgrade;

import com.data.bt.schemaupgrade.celldecoder.SpecificCellDecoderWithOnlyReaderSchema;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.kiji.mapreduce.gather.GathererContext;
import org.kiji.mapreduce.gather.KijiGatherer;
import org.kiji.schema.*;
import org.kiji.schema.impl.HBaseKijiRowData;
import org.kiji.schema.layout.CellSpec;
import org.kiji.schema.layout.KijiTableLayout;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created with IntelliJ IDEA.
 * User: acohen
 * Date: 10/9/13
 * Time: 1:41 PM
 * To change this template use File | Settings | File Templates.
 */
public abstract class SchemaUpgradeGatherer extends KijiGatherer<Text, IntWritable> {

    protected final static Logger LOG = Logger.getLogger(SchemaUpgradeGatherer.class);
    protected Kiji sourceKiji;
    protected Map<KijiColumnName, Schema> columnFamilies;

    public SchemaUpgradeGatherer() {
        columnFamilies = getColumnFamilies();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void gather(KijiRowData kijiRowData, GathererContext<Text, IntWritable> gathererContext) throws IOException {
        HBaseKijiRowData hBaseKijiRowData = (HBaseKijiRowData)kijiRowData;
        KijiTableLayout tableLayout = hBaseKijiRowData.getTableLayout();

        NavigableMap<String, NavigableMap<String, NavigableMap<Long, SpecificRecord>>> deserializedObjects = new TreeMap<>();
        NavigableMap<String, NavigableMap<String, NavigableMap<Long, byte[]>>> rawDataMap = hBaseKijiRowData.getMap();

        for (Map.Entry<String, NavigableMap<String, NavigableMap<Long, byte[]>>> currentFamilyToColumns : rawDataMap.entrySet()) {
            String currentFamilyName = currentFamilyToColumns.getKey();

            for (Map.Entry<String, NavigableMap<Long, byte[]>> currentColumnToTimestamps : currentFamilyToColumns.getValue().entrySet()) {
                String currentColumnName = currentColumnToTimestamps.getKey();

                for (Map.Entry<Long, byte[]> currentCell : currentColumnToTimestamps.getValue().entrySet()) {
                    Long currentCellsTimestamp = currentCell.getKey();
                    byte[] currentCellSerializedData = currentCell.getValue();

                    try {
                        proccessCurrentCell(tableLayout, deserializedObjects, currentFamilyName, currentColumnName, currentCellsTimestamp, currentCellSerializedData);
                    }
                    catch (Exception e) {
                        LOG.error("Failed to convert object with key "
                                + kijiRowData.getEntityId().toString() + ":" + currentFamilyName + ":" + currentColumnName + ":" + currentCellsTimestamp , e);
                    }
                }
            }
        }

        processRow(deserializedObjects, kijiRowData, gathererContext);
    }

    protected void proccessCurrentCell(KijiTableLayout tableLayout, NavigableMap<String, NavigableMap<String, NavigableMap<Long, SpecificRecord>>> deserializedObjects, String currentFamilyName, String currentColumnName, Long currentCellsTimestamp, byte[] currentCellSerializedData) throws IOException {
        KijiCellDecoder<SpecificRecord> kijiCellDecoder = getDecoder(tableLayout, new KijiColumnName(currentFamilyName, currentColumnName));
        DecodedCell<SpecificRecord> decodedCell = kijiCellDecoder.decodeCell(currentCellSerializedData);
        SpecificRecord data = decodedCell.getData();

        NavigableMap<String, NavigableMap<Long, SpecificRecord>> columnNamesToTimestamps = putIfAbsent(deserializedObjects, currentFamilyName, new TreeMap<String, NavigableMap<Long, SpecificRecord>>());
        NavigableMap<Long, SpecificRecord> timestampToDeserializedValues = putIfAbsent(columnNamesToTimestamps, currentColumnName, new TreeMap<Long, SpecificRecord>());
        putIfAbsent(timestampToDeserializedValues, currentCellsTimestamp, data);
    }

    protected <K,V> V putIfAbsent(Map<K, V> destinationMap, K currentKey, V value) {
        V foundValue = destinationMap.get(currentKey);
        if (foundValue == null) {
            destinationMap.put(currentKey, value);
            return value;
        }

        return foundValue;
    }

    protected KijiCellDecoder<SpecificRecord> getDecoder(KijiTableLayout tableLayout, KijiColumnName kijiColumnName) throws IOException {
        Schema schema = columnFamilies.get(kijiColumnName);
        if (schema == null)
            schema = columnFamilies.get(new KijiColumnName(kijiColumnName.getFamily(), null));

        if (schema == null)
            throw new RuntimeException("Cannot find cell decoder for kiji unknown kiji column");

        CellSpec cellSpec = CellSpec.fromCellSchema(tableLayout.getCellSchema(kijiColumnName), sourceKiji.getSchemaTable());
        cellSpec.setReaderSchema(schema);
        return new SpecificCellDecoderWithOnlyReaderSchema<SpecificRecord>(cellSpec);
    }

    @Override
    public Class<?> getOutputKeyClass() {
        return Text.class;
    }

    @Override
    public Class<?> getOutputValueClass() {
        return IntWritable.class;
    }

    @Override
    public KijiDataRequest getDataRequest() {
        KijiDataRequestBuilder builder = KijiDataRequest.builder();
        KijiDataRequestBuilder.ColumnsDef columnsDef = builder.newColumnsDef().withMaxVersions(HConstants.ALL_VERSIONS);

        for (KijiColumnName kijiColumnName : columnFamilies.keySet()) {
            columnsDef.add(kijiColumnName);
        }

        return builder.build();
    }

    protected abstract Map<KijiColumnName, Schema> getColumnFamilies();
    protected abstract void processRow(NavigableMap<String, NavigableMap<String, NavigableMap<Long, SpecificRecord>>> deserializedObjects, KijiRowData kijiRowData, GathererContext<Text, IntWritable> textByteWritableGathererContext) throws IOException;
}
