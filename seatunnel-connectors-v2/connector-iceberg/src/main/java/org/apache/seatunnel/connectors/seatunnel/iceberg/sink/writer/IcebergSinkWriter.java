package org.apache.seatunnel.connectors.seatunnel.iceberg.sink.writer;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.*;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.iceberg.IcebergTableLoader;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.SinkConfig;
import org.apache.seatunnel.connectors.seatunnel.iceberg.data.DataConverter;
import org.apache.seatunnel.connectors.seatunnel.iceberg.data.DefaultDataConverter;

import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.util.ArrayUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

@Slf4j
public class IcebergSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    private final static String APPEND = "append";
    private SinkWriter.Context context;

    private Schema tableSchema;

    private SeaTunnelRowType seaTunnelRowType;
    private IcebergTableLoader icebergTableLoader;
    private SinkConfig sinkConfig;
    private Table table;

    private List<Record> pendingRows = new ArrayList<>();
    private DataConverter defaultDataConverter;

    private static final int FORMAT_V2 = 2;

    private final FileFormat format;

    private PartitionKey partition = null;
    private OutputFileFactory fileFactory = null;

    private String mode = APPEND;

    private Boolean deleteData = false;

    private long startTime = System.currentTimeMillis();

    public IcebergSinkWriter(
            SinkWriter.Context context,
            Schema tableSchema,
            SeaTunnelRowType seaTunnelRowType,
            SinkConfig sinkConfig) {
        this.context = context;
        this.sinkConfig = sinkConfig;
        this.tableSchema = tableSchema;
        this.seaTunnelRowType = seaTunnelRowType;
        defaultDataConverter = new DefaultDataConverter(seaTunnelRowType, tableSchema);
        if (icebergTableLoader == null) {
            icebergTableLoader = IcebergTableLoader.create(sinkConfig);
            icebergTableLoader.open();
        }
        if (table == null) {
            table = icebergTableLoader.loadTable();
        }
        this.format = FileFormat.valueOf(sinkConfig.getFileFormat().toUpperCase(Locale.ENGLISH));
        this.fileFactory = OutputFileFactory.builderFor(table, 1, 1).format(format).build();
        this.mode = sinkConfig.getMode();
    }

    private PartitionKey createPartitionKey(Record record) {
        if (table.spec().isUnpartitioned()) {
            return null;
        }
        PartitionKey partitionKey = new PartitionKey(table.spec(), table.schema());
        partitionKey.partition(record);
        return partitionKey;
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        Record record = defaultDataConverter.toIcebergStruct(element);
        pendingRows.add(record);
        this.partition = createPartitionKey(record);
        if(!deleteData && !APPEND.equalsIgnoreCase(mode)){
            DeleteFiles deleteFiles = table.newDelete();
            if(partition!=null){
                deleteFiles.deleteFromRowFilter(buildPartitionFilter());
            }else{
                deleteFiles.deleteFromRowFilter(null);
            }
            deleteFiles.commit();
            log.info("数据删除成功");
            deleteData = true;
        }

        long endTime = System.currentTimeMillis();
        Boolean arrivalSubmissionTime = startTime !=0 && (endTime-startTime)/1000 > sinkConfig.getCommitInterval() ? true : false;

        if (pendingRows.size() >= sinkConfig.getMaxRow() || arrivalSubmissionTime) {
            commitDataToIceberg();
        }
    }

    private Expression buildPartitionFilter() {
        Expression result = null;
        List<PartitionField> fields = table.spec().fields();
        for (int i=0;i<fields.size();i++){
            Expression equal = Expressions.equal(fields.get(i).name(), partition.get(i, String.class));
            if (result == null) {
                result = equal;
            } else {
                result = Expressions.and(result, equal);
            }
        }
        return result;
    }

    private void commitDataToIceberg() throws IOException {
        FileAppenderFactory<Record> appenderFactory = createAppenderFactory(null, null, null);
        DataFile dataFile = prepareDataFile(pendingRows, appenderFactory);
        table.newRowDelta().addRows(dataFile).commit();
        pendingRows.clear();
        startTime = System.currentTimeMillis();
    }

    private DataFile prepareDataFile(
            List<Record> rowSet, FileAppenderFactory<Record> appenderFactory) throws IOException {
        DataWriter<Record> writer =
                appenderFactory.newDataWriter(createEncryptedOutputFile(), format, partition);

        try (DataWriter<Record> closeableWriter = writer) {
            for (Record row : rowSet) {
                closeableWriter.write(row);
            }
        }

        return writer.toDataFile();
    }

    private EncryptedOutputFile createEncryptedOutputFile() {
        if (partition == null) {
            return fileFactory.newOutputFile();
        } else {
            return fileFactory.newOutputFile(partition);
        }
    }

    protected FileAppenderFactory<Record> createAppenderFactory(
            List<Integer> equalityFieldIds, Schema eqDeleteSchema, Schema posDeleteRowSchema) {
        return new GenericAppenderFactory(
                table.schema(),
                table.spec(),
                ArrayUtil.toIntArray(equalityFieldIds),
                eqDeleteSchema,
                posDeleteRowSchema);
    }

    @SneakyThrows
    @Override
    public Optional<Void> prepareCommit() {
        if(pendingRows.size()>0){
            commitDataToIceberg();
        }
        return super.prepareCommit();
    }


    @Override
    public void close() throws IOException {
        if (icebergTableLoader != null) {
            icebergTableLoader.close();
            pendingRows.clear();
        }
    }
}
