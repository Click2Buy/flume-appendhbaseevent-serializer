package com.marketconnect.flume.serializer;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.sink.hbase2.HBase2EventSerializer;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import com.google.common.collect.Lists;

public class AppendHBase2EventSerializer implements HBase2EventSerializer {
  // Config vars

  /** Column name to place match group in. */
  public static final String COL_NAME_CONFIG = "colName";
  public static final String COLUMN_NAME_DEFAULT = "col";

  /** What charset to use when serializing into HBase's byte arrays */
  public static final String CHARSET_CONFIG = "charset";
  public static final String CHARSET_DEFAULT = "UTF-8";

  protected byte[] cf;
  private byte[] payload;
  private String colName;
  private Map<String, String> headers;
  private Charset charset;

  @Override
  public void configure(Context context) {
    charset = Charset.forName(context.getString(CHARSET_CONFIG,
        CHARSET_DEFAULT));

    colName = context.getString(COL_NAME_CONFIG, COLUMN_NAME_DEFAULT);
  }

  @Override
  public void configure(ComponentConfiguration conf) {
  }

  @Override
  public void initialize(Event event, byte[] columnFamily) {
    this.headers = event.getHeaders();
    this.payload = event.getBody();
    this.cf = Arrays.copyOf(columnFamily, columnFamily.length);
  }

  @Override
  public List<Row> getActions() throws FlumeException {
    List<Row> actions = Lists.newArrayList();
    byte[] rowKey = this.payload;

    if (rowKey.length == 0) {
      return Lists.newArrayList();
    }

    try {
      Append append = new Append(rowKey);

      String valueStr = headers.get(colName);
      append.add(cf, colName.getBytes(charset), Bytes.toBytes(valueStr));
      actions.add(append);
    } catch (IllegalArgumentException e) {
      throw new FlumeException(e + " row key " + Bytes.toString(rowKey));
    } catch (Exception e) {
      throw new FlumeException(e);
    }
    return actions;
  }

  @Override
  public List<Increment> getIncrements() {
    return Lists.newArrayList();
  }

  @Override
  public void close() {  }
}
