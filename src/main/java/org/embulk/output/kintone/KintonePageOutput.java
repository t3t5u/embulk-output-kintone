package org.embulk.output.kintone;

import com.kintone.client.KintoneClient;
import com.kintone.client.KintoneClientBuilder;
import com.kintone.client.api.record.GetRecordsByCursorResponseBody;
import com.kintone.client.model.app.field.FieldProperty;
import com.kintone.client.model.record.FieldType;
import com.kintone.client.model.record.Record;
import com.kintone.client.model.record.RecordForUpdate;
import com.kintone.client.model.record.UpdateKey;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.embulk.config.ConfigException;
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
  private static final int CHUNK_SIZE = 100;
  private final PluginTask task;
  private final PageReader reader;
  private KintoneClient client;
  private Map<String, FieldProperty> formFields;

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
    if (client != null) {
      return;
    }
    KintoneClientBuilder builder = KintoneClientBuilder.create("https://" + task.getDomain());
    if (task.getGuestSpaceId().isPresent()) {
      builder.setGuestSpaceId(task.getGuestSpaceId().get());
    }
    if (task.getBasicAuthUsername().isPresent() && task.getBasicAuthPassword().isPresent()) {
      builder.withBasicAuth(task.getBasicAuthUsername().get(), task.getBasicAuthPassword().get());
    }
    if (task.getUsername().isPresent() && task.getPassword().isPresent()) {
      client = builder.authByPassword(task.getUsername().get(), task.getPassword().get()).build();
    } else if (task.getToken().isPresent()) {
      client = builder.authByApiToken(task.getToken().get()).build();
    }
    if (client != null) {
      formFields = client.app().getFormFields(task.getAppId());
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
                    reader, task.getColumnOptions(), task.getPreferNulls(), task.getIgnoreNulls());
            while (reader.nextRecord()) {
              Record record = new Record();
              visitor.setRecord(record);
              reader.getSchema().visitColumns(visitor);
              records.add(record);
              if (records.size() == CHUNK_SIZE) {
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
              if (updateRecords.size() == CHUNK_SIZE) {
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
    List<Object> existingValues = getExistingValuesByUpdateKey(updateKeys);
    ArrayList<Record> insertRecords = new ArrayList<>();
    ArrayList<RecordForUpdate> updateRecords = new ArrayList<>();
    for (int i = 0; i < records.size(); i++) {
      Record record = records.get(i);
      UpdateKey updateKey = updateKeys.get(i);
      if (existsRecord(existingValues, updateKey)) {
        updateRecords.add(new RecordForUpdate(updateKey, record.removeField(updateKey.getField())));
      } else {
        insertRecords.add(record);
      }
      if (insertRecords.size() == CHUNK_SIZE) {
        client.record().addRecords(task.getAppId(), insertRecords);
        insertRecords.clear();
      } else if (updateRecords.size() == CHUNK_SIZE) {
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

  private List<Object> getExistingValuesByUpdateKey(ArrayList<UpdateKey> updateKeys) {
    String fieldCode =
        updateKeys.stream()
            .map(UpdateKey::getField)
            .filter(Objects::nonNull)
            .findFirst()
            .orElse(null);
    if (fieldCode == null) {
      return Collections.emptyList();
    }
    FieldType fieldType = formFields.get(fieldCode).getType();
    if (!Arrays.asList(FieldType.SINGLE_LINE_TEXT, FieldType.NUMBER).contains(fieldType)) {
      throw new ConfigException("The update_key must be 'SINGLE_LINE_TEXT' or 'NUMBER'.");
    }
    List<String> queryValues =
        updateKeys.stream()
            .filter(k -> k.getValue() != null && !k.getValue().toString().isEmpty())
            .map(k -> "\"" + k.getValue() + "\"")
            .collect(Collectors.toList());
    if (queryValues.isEmpty()) {
      return Collections.emptyList();
    }
    String cursorId =
        client
            .record()
            .createCursor(
                task.getAppId(),
                Collections.singletonList(fieldCode),
                fieldCode + " in (" + String.join(",", queryValues) + ")");
    List<Record> allRecords = new ArrayList<>();
    while (true) {
      GetRecordsByCursorResponseBody resp = client.record().getRecordsByCursor(cursorId);
      List<Record> records = resp.getRecords();
      allRecords.addAll(records);
      if (!resp.hasNext()) {
        break;
      }
    }
    return allRecords.stream()
        .map(
            (r) -> {
              switch (fieldType) {
                case SINGLE_LINE_TEXT:
                  return r.getSingleLineTextFieldValue(fieldCode);
                case NUMBER:
                  return r.getNumberFieldValue(fieldCode);
                default:
                  return null;
              }
            })
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  private boolean existsRecord(List<Object> existingValues, UpdateKey updateKey) {
    return existingValues.stream().anyMatch(v -> v.equals(updateKey.getValue()));
  }
}
