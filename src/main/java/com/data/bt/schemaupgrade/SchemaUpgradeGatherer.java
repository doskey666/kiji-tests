package com.data.bt.schemaupgrade;

import com.data.bt.schemaupgrade.celldecoder.SpecificCellDecoderWithOnlyReaderSchema;
import com.data.bt.utils.MapUtils;
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

/**
 * Created with IntelliJ IDEA.
 * User: acohen
 * Date: 10/9/13
 * Time: 1:41 PM
 * An abstract Gatherer used to read cells with overrides to their WriterSchemas
 */
public abstract class SchemaUpgradeGatherer extends KijiGatherer<Text, IntWritable> {

    protected final static Logger LOG = Logger.getLogger(SchemaUpgradeGatherer.class);
    protected Kiji sourceKiji;
    protected Map<KijiColumnName, Schema> columnFamilies;

    public SchemaUpgradeGatherer() {
        columnFamilies = getColumnSchemaOverrides();
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
                        processCurrentCell(tableLayout, deserializedObjects, currentFamilyName, currentColumnName, currentCellsTimestamp, currentCellSerializedData);
                        gathererContext.incrementCounter(SchemaUpgradeGathererCounter.SUCCESSFUL_CONVERSIONS);
                    }
                    catch (Exception e) {
                        gathererContext.incrementCounter(SchemaUpgradeGathererCounter.FAILED_CONVERSIONS);
                        LOG.error("Failed to convert object with key "
                                + kijiRowData.getEntityId().toString() + ":" + currentFamilyName + ":" + currentColumnName + ":" + currentCellsTimestamp , e);
                    }
                }
            }
        }

        processRow(deserializedObjects, kijiRowData, gathererContext);
    }

    protected void processCurrentCell(KijiTableLayout tableLayout, NavigableMap<String, NavigableMap<String, NavigableMap<Long, SpecificRecord>>> deserializedObjects, String currentFamilyName, String currentColumnName, Long currentCellsTimestamp, byte[] currentCellSerializedData) throws IOException {
        KijiCellDecoder<SpecificRecord> kijiCellDecoder = getDecoder(tableLayout, new KijiColumnName(currentFamilyName, currentColumnName));
        DecodedCell<SpecificRecord> decodedCell = kijiCellDecoder.decodeCell(currentCellSerializedData);
        SpecificRecord data = decodedCell.getData();

        NavigableMap<String, NavigableMap<Long, SpecificRecord>> columnNamesToTimestamps = MapUtils.putIfAbsent(deserializedObjects, currentFamilyName, new TreeMap<String, NavigableMap<Long, SpecificRecord>>());
        NavigableMap<Long, SpecificRecord> timestampToDeserializedValues = MapUtils.putIfAbsent(columnNamesToTimestamps, currentColumnName, new TreeMap<Long, SpecificRecord>());
        MapUtils.putIfAbsent(timestampToDeserializedValues, currentCellsTimestamp, data);
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

    /**
     * Gets a mapping between a KijiColumnName and a schema to use when deserializing it
     * Specified KijiColumnNames Will also be used in the generated KijiDataRequest
     * @return
     */
    protected abstract Map<KijiColumnName, Schema> getColumnSchemaOverrides();

    /**
     * A delegate of the "gather" function with the addition of current row cells deserialized using the overrides specified in getColumnSchemaOverrides
     * @param deserializedObjects
     * @param kijiRowData
     * @param textByteWritableGathererContext
     * @throws IOException
     */
    protected abstract void processRow(NavigableMap<String, NavigableMap<String, NavigableMap<Long, SpecificRecord>>> deserializedObjects, KijiRowData kijiRowData, GathererContext<Text, IntWritable> textByteWritableGathererContext) throws IOException;
}
