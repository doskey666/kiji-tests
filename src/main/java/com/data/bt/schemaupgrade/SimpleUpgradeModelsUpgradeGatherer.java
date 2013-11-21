package com.data.bt.schemaupgrade;

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
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created with IntelliJ IDEA.
 * User: acohen
 * Date: 10/9/13
 * Time: 3:36 PM
 * Upgrade person table job
 */
public class SimpleUpgradeModelsUpgradeGatherer extends SchemaUpgradeGatherer {

    protected final static Logger LOG = Logger.getLogger(SimpleUpgradeModelsUpgradeGatherer.class);

    @Override
    @SuppressWarnings("unchecked")
    public void setup(GathererContext<Text, IntWritable> context) throws IOException {
        super.setup(context);
        this.sourceKiji = Kiji.Factory.open(KijiURI.newBuilder("kiji://.env/test").build());
    }

    @Override
    protected Map<KijiColumnName, Schema> getColumnFamilies() {
//        return ImmutableMap.of(new KijiColumnName("blueforce_requests", null), BlueForceRequest.SCHEMA$,
//                new KijiColumnName("reflections", null), PersonReflection.SCHEMA$,
//                new KijiColumnName("candidates", "candidate_list"), CandidateList.SCHEMA$);
        return new HashMap<KijiColumnName, Schema>();
    }

    @Override
    protected void processRow(NavigableMap<String, NavigableMap<String,NavigableMap<Long,SpecificRecord>>> deserializedObjects, KijiRowData kijiRowData, GathererContext<Text, IntWritable> textByteWritableGathererContext) throws IOException {
        Object oldVersionModel = buildModel(deserializedObjects.values());
        Object newVersionModel = convertModel(oldVersionModel);
        verify(newVersionModel);
        sendToDataServices(newVersionModel);
    }

    private void sendToDataServices(Object newVersionModel) {
        //To change body of created methods use File | Settings | File Templates.
    }

    private void verify(Object newVersionModel) {
    }

    private Object convertModel(Object oldVersionModel) {
        return null;  //To change body of created methods use File | Settings | File Templates.
    }

    private Object buildModel(Collection<NavigableMap<String, NavigableMap<Long, SpecificRecord>>> values) {
        return null;  //To change body of created methods use File | Settings | File Templates.
    }

    @Override
    public void cleanup(GathererContext<Text, IntWritable> context) throws IOException {
        if (sourceKiji != null)
            sourceKiji.release();

        super.cleanup(context);
    }

}
