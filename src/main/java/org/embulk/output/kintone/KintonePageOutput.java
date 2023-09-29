package org.embulk.output.kintone;

import com.kintone.client.KintoneClient;
import com.kintone.client.KintoneClientBuilder;
import com.kintone.client.api.record.GetRecordsByCursorResponseBody;
import com.kintone.client.model.record.FieldType;
import com.kintone.client.model.record.Record;
import com.kintone.client.model.record.RecordForUpdate;
import com.kintone.client.model.record.UpdateKey;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.embulk.config.TaskReport;
import org.embulk.spi.Exec;
import org.embulk.spi.Page;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KintonePageOutput implements TransactionalPageOutput {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final int UPSERT_BATCH_SIZE = 10000;
  private final PluginTask task;
  private final PageReader reader;
  private KintoneClient client;

  public KintonePageOutput(PluginTask task, Schema schema) {
    this.task = task;
    reader = new PageReader(schema);
  }

  @Override
  public void add(Page page) {
    KintoneMode mode = KintoneMode.getKintoneModeByValue(task.getMode());
    switch (mode) {
      case INSERT:
        insertPage(page);
        break;
      case UPDATE:
        updatePage(page);
        break;
      case UPSERT:
        upsertPage(page);
        break;
      default:
        throw new UnsupportedOperationException(String.format("Unknown mode '%s'", task.getMode()));
    }
  }

  @Override
  public void finish() {
    // noop
  }

  @Override
  public void close() {
    if (client == null) {
      return;
    }
    try {
      client.close();
    } catch (Exception e) {
      throw new RuntimeException("kintone throw exception", e);
    }
  }

  @Override
  public void abort() {
    // noop
  }

  @Override
  public TaskReport commit() {
    return Exec.newTaskReport();
  }

  public void connect(final PluginTask task) {
    KintoneClientBuilder builder = KintoneClientBuilder.create("https://" + task.getDomain());
    if (task.getGuestSpaceId().isPresent()) {
      builder.setGuestSpaceId(task.getGuestSpaceId().orElse(-1));
    }
    if (task.getBasicAuthUsername().isPresent() && task.getBasicAuthPassword().isPresent()) {
      builder.withBasicAuth(task.getBasicAuthUsername().get(), task.getBasicAuthPassword().get());
    }
    if (task.getUsername().isPresent() && task.getPassword().isPresent()) {
      client = builder.authByPassword(task.getUsername().get(), task.getPassword().get()).build();
    } else if (task.getToken().isPresent()) {
      client = builder.authByApiToken(task.getToken().get()).build();
    }
  }

  private void execute(Consumer<KintoneClient> operation) {
    connect(task);
    if (client != null) {
      operation.accept(client);
    } else {
      throw new RuntimeException("Failed to connect to kintone.");
    }
  }

  private void insertPage(final Page page) {
    execute(
        client -> {
          try {
            ArrayList<Record> records = new ArrayList<>();
            reader.setPage(page);
            KintoneColumnVisitor visitor =
                new KintoneColumnVisitor(
                    reader,
                    task.getColumnOptions(),
                    task.getPreferNulls(),
                    task.getIgnoreNulls(),
                    task.getReduceKeyName().orElse(null));
            while (reader.nextRecord()) {
              Record record = new Record();
              visitor.setRecord(record);
              reader.getSchema().visitColumns(visitor);
              records.add(record);
              if (records.size() == task.getChunkSize()) {
                client.record().addRecords(task.getAppId(), records);
                records.clear();
              }
            }
            if (!records.isEmpty()) {
              client.record().addRecords(task.getAppId(), records);
            }
          } catch (Exception e) {
            throw new RuntimeException("kintone throw exception", e);
          }
        });
  }

  private void updatePage(final Page page) {
    execute(
        client -> {
          try {
            ArrayList<RecordForUpdate> updateRecords = new ArrayList<>();
            reader.setPage(page);
            KintoneColumnVisitor visitor =
                new KintoneColumnVisitor(
                    reader,
                    task.getColumnOptions(),
                    task.getPreferNulls(),
                    task.getIgnoreNulls(),
                    task.getReduceKeyName().orElse(null),
                    task.getUpdateKeyName()
                        .orElseThrow(
                            () -> new RuntimeException("unreachable"))); // Already validated
            while (reader.nextRecord()) {
              Record record = new Record();
              UpdateKey updateKey = new UpdateKey();
              visitor.setRecord(record);
              visitor.setUpdateKey(updateKey);
              reader.getSchema().visitColumns(visitor);
              if (updateKey.getValue() == null || updateKey.getValue().toString().isEmpty()) {
                LOGGER.warn("Record skipped because no update key value was specified");
                continue;
              }
              updateRecords.add(
                  new RecordForUpdate(updateKey, record.removeField(updateKey.getField())));
              if (updateRecords.size() == task.getChunkSize()) {
                client.record().updateRecords(task.getAppId(), updateRecords);
                updateRecords.clear();
              }
            }
            if (!updateRecords.isEmpty()) {
              client.record().updateRecords(task.getAppId(), updateRecords);
            }
          } catch (Exception e) {
            throw new RuntimeException("kintone throw exception", e);
          }
        });
  }

  private void upsertPage(final Page page) {
    execute(
        client -> {
          try {
            ArrayList<Record> records = new ArrayList<>();
            ArrayList<UpdateKey> updateKeys = new ArrayList<>();
            reader.setPage(page);
            KintoneColumnVisitor visitor =
                new KintoneColumnVisitor(
                    reader,
                    task.getColumnOptions(),
                    task.getPreferNulls(),
                    task.getIgnoreNulls(),
                    task.getReduceKeyName().orElse(null),
                    task.getUpdateKeyName()
                        .orElseThrow(
                            () -> new RuntimeException("unreachable"))); // Already validated
            while (reader.nextRecord()) {
              Record record = new Record();
              UpdateKey updateKey = new UpdateKey();
              visitor.setRecord(record);
              visitor.setUpdateKey(updateKey);
              reader.getSchema().visitColumns(visitor);
              records.add(record);
              updateKeys.add(updateKey);
              if (records.size() == UPSERT_BATCH_SIZE) {
                upsert(records, updateKeys);
                records.clear();
                updateKeys.clear();
              }
            }
            if (!records.isEmpty()) {
              upsert(records, updateKeys);
            }
          } catch (Exception e) {
            throw new RuntimeException("kintone throw exception", e);
          }
        });
  }

  private void upsert(ArrayList<Record> records, ArrayList<UpdateKey> updateKeys) {
    if (records.size() != updateKeys.size()) {
      throw new RuntimeException("records.size() != updateKeys.size()");
    }
    List<Record> existingRecords = getExistingRecordsByUpdateKey(updateKeys);
    ArrayList<Record> insertRecords = new ArrayList<>();
    ArrayList<RecordForUpdate> updateRecords = new ArrayList<>();
    for (int i = 0; i < records.size(); i++) {
      Record record = records.get(i);
      UpdateKey updateKey = updateKeys.get(i);
      if (existsRecord(existingRecords, updateKey)) {
        updateRecords.add(new RecordForUpdate(updateKey, record.removeField(updateKey.getField())));
      } else {
        insertRecords.add(record);
      }
      if (insertRecords.size() == task.getChunkSize()) {
        client.record().addRecords(task.getAppId(), insertRecords);
        insertRecords.clear();
      } else if (updateRecords.size() == task.getChunkSize()) {
        client.record().updateRecords(task.getAppId(), updateRecords);
        updateRecords.clear();
      }
    }
    if (!insertRecords.isEmpty()) {
      client.record().addRecords(task.getAppId(), insertRecords);
    }
    if (!updateRecords.isEmpty()) {
      client.record().updateRecords(task.getAppId(), updateRecords);
    }
  }

  private List<Record> getExistingRecordsByUpdateKey(ArrayList<UpdateKey> updateKeys) {
    String fieldCode =
        updateKeys.stream()
            .map(UpdateKey::getField)
            .filter(Objects::nonNull)
            .findFirst()
            .orElse(null);
    if (fieldCode == null) {
      return Collections.emptyList();
    }
    List<String> queryValues =
        updateKeys.stream()
            .filter(k -> k.getValue() != null && !k.getValue().toString().isEmpty())
            .map(k -> "\"" + k.getValue() + "\"")
            .collect(Collectors.toList());
    List<Record> allRecords = new ArrayList<>();
    if (queryValues.isEmpty()) {
      return allRecords;
    }
    String cursorId =
        client
            .record()
            .createCursor(
                task.getAppId(),
                Collections.singletonList(fieldCode),
                fieldCode + " in (" + String.join(",", queryValues) + ")");
    while (true) {
      GetRecordsByCursorResponseBody resp = client.record().getRecordsByCursor(cursorId);
      List<Record> records = resp.getRecords();
      allRecords.addAll(records);
      if (!resp.hasNext()) {
        break;
      }
    }
    return allRecords;
  }

  private boolean existsRecord(List<Record> distRecords, UpdateKey updateKey) {
    if (updateKey.getValue() == null || updateKey.getValue().toString().isEmpty()) {
      return false;
    }
    String fieldCode = updateKey.getField();
    FieldType type = client.app().getFormFields(task.getAppId()).get(fieldCode).getType();
    switch (type) {
      case SINGLE_LINE_TEXT:
        return distRecords.stream()
            .anyMatch(d -> d.getSingleLineTextFieldValue(fieldCode).equals(updateKey.getValue()));
      case NUMBER:
        return distRecords.stream()
            .anyMatch(d -> d.getNumberFieldValue(fieldCode).equals(updateKey.getValue()));
      default:
        throw new RuntimeException("The update_key must be 'SINGLE_LINE_TEXT' or 'NUMBER'.");
    }
  }
}
