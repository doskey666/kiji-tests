package com.data.bt.schemaupgrade;

import com.google.common.collect.ImmutableMap;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.kiji.mapreduce.gather.GathererContext;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiURI;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.NavigableMap;

/**
 * Created with IntelliJ IDEA.
 * User: acohen
 * Date: 10/9/13
 * Time: 3:36 PM
 * Upgrade person table job
 */
public class SimpleUpgradeModelsUpgradeGatherer extends SchemaUpgradeGatherer {

    @Override
    @SuppressWarnings("unchecked")
    public void setup(GathererContext<Text, IntWritable> context) throws IOException {
        super.setup(context);
        this.sourceKiji = Kiji.Factory.open(KijiURI.newBuilder("kiji://.env/test").build());
    }

    @Override
    protected Map<KijiColumnName, Schema> getColumnSchemaOverrides() {
        return ImmutableMap.of(new KijiColumnName("attributes", null), com.data.bt.models.avro.SimpleUpgradeModelsEntity.SCHEMA$);
    }

    @Override
    protected void processRow(NavigableMap<String, NavigableMap<String,NavigableMap<Long,SpecificRecord>>> deserializedObjects, KijiRowData kijiRowData, GathererContext<Text, IntWritable> textByteWritableGathererContext) throws IOException {
        Object oldVersionModel = buildModel(deserializedObjects.values());
        Object newVersionModel = convertModel(oldVersionModel);
        verify(newVersionModel);
        sendToDataServices(newVersionModel);
    }

    protected void sendToDataServices(Object newVersionModel) {
    }

    protected void verify(Object newVersionModel) {
    }

    protected Object convertModel(Object oldVersionModel) {
        return null;
    }

    protected Object buildModel(Collection<NavigableMap<String, NavigableMap<Long, SpecificRecord>>> deserializedObjects) {
        return null;
    }

    @Override
    public void cleanup(GathererContext<Text, IntWritable> context) throws IOException {
        if (sourceKiji != null)
            sourceKiji.release();

        super.cleanup(context);
    }

}
