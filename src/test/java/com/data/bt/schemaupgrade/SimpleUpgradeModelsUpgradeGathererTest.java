package com.data.bt.schemaupgrade;

import com.data.bt.models.avro.legacy.LegacySimpleUpgradeModelsAttribute;
import com.data.bt.models.avro.legacy.LegacySimpleUpgradeModelsAttributeType;
import com.data.bt.models.avro.legacy.LegacySimpleUpgradeModelsEntity;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.junit.Test;
import org.kiji.mapreduce.KijiMapReduceJob;
import org.kiji.mapreduce.gather.KijiGatherJobBuilder;
import org.kiji.mapreduce.output.MapReduceJobOutputs;
import org.kiji.schema.*;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.layout.KijiTableLayout;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Created with IntelliJ IDEA.
 * User: acohen
 * Date: 11/21/13
 * Time: 2:48 PM
 * To change this template use File | Settings | File Templates.
 */
public class SimpleUpgradeModelsUpgradeGathererTest {
    protected static final String TABLE_NAME = "SimpleUpgradeModelsTest";
    protected static final String COLUMN_FAMILY = "entity";
    public static final String COLUMN_QUALIFIER = "someColumnQualifier";
    protected static final String TABLE_LAYOUT = "/SimpleUpgradeModelsTest_layout.json";

    @Test
    public void testReadingWithDifferentWriterSchema() throws Exception {
        String kijiUrl = "kiji://.env/simple_upgrade_test_schema";
        prepareSchema(kijiUrl);
        runUpgrade(kijiUrl);
    }

    protected void prepareSchema(String kijiUrl) throws Exception {
        Kiji kiji = initTable(kijiUrl);
        KijiTable simpleUpgradeModelsTestTable = kiji.openTable(TABLE_NAME);
        KijiTableWriter tableWriter = simpleUpgradeModelsTestTable.openTableWriter();
        HBaseEntityId rowKey = HBaseEntityId.fromHBaseRowKey(Bytes.toBytes("rowKey"));
        tableWriter.put(rowKey, COLUMN_FAMILY, COLUMN_QUALIFIER, createLegacyEntity());
        tableWriter.close();

        KijiTableReader kijiTableReader = simpleUpgradeModelsTestTable.openTableReader();
        KijiRowData entity = kijiTableReader.get(rowKey, KijiDataRequest.create("entity", null));
        LegacySimpleUpgradeModelsEntity fetchedData = entity.getMostRecentValue(COLUMN_FAMILY, COLUMN_QUALIFIER);
        assertEquals(2, fetchedData.getAttributes().size());
        kiji.release();
    }

    protected void runUpgrade(String kijiUrl) throws IOException, InterruptedException, ClassNotFoundException {
        KijiURI inputTable = KijiURI.newBuilder(kijiUrl + "/" + TABLE_NAME).build();

        Configuration conf = HBaseConfiguration.create();
        conf.setInt("hbase.client.scanner.caching", 100);

        FileSystem fileSystem = FileSystem.get(conf);
        Path outputPath = new Path("/tmp/output");
        fileSystem.delete(outputPath, true);

        KijiMapReduceJob build = KijiGatherJobBuilder.create()
                .withConf(conf)
                .withInputTable(inputTable)
                .withOutput(MapReduceJobOutputs.newTextMapReduceJobOutput(outputPath, 1))
                .withGatherer(SimpleUpgradeModelsUpgradeGatherer.class).build();
        try {
            build.run();
            verifyResults(build);
        }
        finally {
            fileSystem.close();
        }
    }

    protected void verifyResults(KijiMapReduceJob build) throws IOException {
        Counters counters = build.getHadoopJob().getCounters();
        Counter sucessfulCounter = counters.findCounter(SchemaUpgradeGathererCounter.SUCCESSFUL_CONVERSIONS);
        assertEquals(1, sucessfulCounter.getValue());
        Counter failedCounter = counters.findCounter(SchemaUpgradeGathererCounter.FAILED_CONVERSIONS);
        assertEquals(0, failedCounter.getValue());
    }

    protected LegacySimpleUpgradeModelsEntity createLegacyEntity() {
        LegacySimpleUpgradeModelsAttribute firstAttribute = LegacySimpleUpgradeModelsAttribute.newBuilder()
                .setAttributeType(LegacySimpleUpgradeModelsAttributeType.personName)
                .setAttributeValue("a name").build();
        LegacySimpleUpgradeModelsAttribute secondAttribute = LegacySimpleUpgradeModelsAttribute.newBuilder()
                .setAttributeType(LegacySimpleUpgradeModelsAttributeType.personName)
                .setAttributeValue("a different name").build();

        return LegacySimpleUpgradeModelsEntity.newBuilder()
                .setAttributes(Lists.newArrayList(firstAttribute,secondAttribute))
                .setSomeOtherData("blabla")
                .build();
    }

    protected Kiji initTable(String kijiUrl) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        KijiURI kijiURI = KijiURI.newBuilder(kijiUrl).build();

        KijiInstaller kijiInstaller = KijiInstaller.get();
        try {
            kijiInstaller.install(kijiURI, conf);
        }
        catch (KijiAlreadyExistsException e) {
            kijiInstaller.uninstall(kijiURI, conf);
            kijiInstaller.install(kijiURI, conf);
            Kiji kiji = Kiji.Factory.open(kijiURI, conf);
            if (kiji.getTableNames().contains(TABLE_NAME))
                kiji.deleteTable(TABLE_NAME);
        }

        Kiji kiji = Kiji.Factory.open(kijiURI, conf);
        TableLayoutDesc tableLayoutDesc = KijiTableLayout.createFromEffectiveJsonResource(TABLE_LAYOUT).getDesc();
        kiji.createTable(tableLayoutDesc);
        return kiji;
    }
}
