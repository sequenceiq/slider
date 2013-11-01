/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hoya.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.SeekableFileInput;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.fs.AvroFSInput;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hoya.yarn.appmaster.state.NodeEntry;
import org.apache.hadoop.hoya.yarn.appmaster.state.NodeInstance;
import org.apache.hadoop.hoya.yarn.appmaster.state.RoleHistory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;

/**
 * Write out the role history to an output stream
 */
public class RoleHistoryWriter {
  protected static final Logger log =
    LoggerFactory.getLogger(RoleHistoryWriter.class);

  /**
   * Write out the history.
   * This does not update the history's dirty/savetime fields
   *
   * @param out outstream
   * @param history history
   * @param savetime time in millis for the save time to go in as a record
   * @return no of records written
   * @throws IOException IO failures
   */
  public long write(OutputStream out, RoleHistory history, long savetime)
    throws IOException {
    try {
      DatumWriter<RoleHistoryRecord> writer =
        new SpecificDatumWriter<RoleHistoryRecord>(RoleHistoryRecord.class);

      int roles = history.getRoleSize();
      RoleHistoryHeader header = new RoleHistoryHeader(savetime,
                                                       roles);
      RoleHistoryRecord record = new RoleHistoryRecord(header);
      Schema schema = record.getSchema();
      Encoder encoder = EncoderFactory.get().jsonEncoder(schema, out);
      writer.write(record, encoder);
      long count = 0;
      //now for every role history entry, write out its record
      Collection<NodeInstance> instances = history.cloneNodemap().values();
      for (NodeInstance instance : instances) {
        for (int role = 0; role < roles; role++) {
          NodeEntry nodeEntry = instance.get(role);

          if (nodeEntry != null) {
            NodeEntryRecord ner = build(nodeEntry, role, instance.nodeAddress);
            record = new RoleHistoryRecord(ner);
            writer.write(record, encoder);
            count++;
          }
        }
      }
      // footer
      RoleHistoryFooter footer = new RoleHistoryFooter(count);
      writer.write(new RoleHistoryRecord(footer), encoder);
      encoder.flush();
      out.close();
      return count;
    } finally {
      out.close();
    }
  }

  /**
   * Write write the file
   *
   *
   * @param fs filesystem
   * @param path path
   * @param overwrite overwrite flag
   * @param history history
   * @param savetime time in millis for the save time to go in as a record
   * @return no of records written
   * @throws IOException IO failures
   */
  public long write(FileSystem fs, Path path, boolean overwrite,
                    RoleHistory history, long savetime) throws IOException {
    FSDataOutputStream out = fs.create(path, overwrite);
    return write(out, history, savetime);
  }

  private NodeEntryRecord build(NodeEntry entry, int role, NodeAddress ref) {
    NodeEntryRecord record = new NodeEntryRecord(
      ref, role, entry.getLive() > 0, entry.getLastUsed()
    );
    return record;
  }

  /**
   * Read a history, returning one that is ready to have its onThaw() 
   * method called
   * @param in input source
   * @param history a history set up with the expected roles; 
   * this will be built up with a node map configured with the node instances
   * and entries loaded from the source
   * @return no. of entries read
   * @throws IOException problems
   */
  public int read(InputStream in, RoleHistory history) throws IOException {
    try {
      DatumReader<RoleHistoryRecord> reader =
        new SpecificDatumReader<RoleHistoryRecord>(RoleHistoryRecord.class);
      Decoder decoder =
        DecoderFactory.get().jsonDecoder(RoleHistoryRecord.getClassSchema(),
                                         in);

      //read header : no entry -> EOF
      RoleHistoryRecord record = reader.read(null, decoder);
      Object entry = record.getEntry();
      if (!(entry instanceof RoleHistoryHeader)) {
        throw new IOException("Role History Header not found at start of file");
      }
      RoleHistoryHeader header = (RoleHistoryHeader) entry;
      Integer roleSize = header.getRoles();
      Long saved = header.getSaved();
      history.prepareForReading(roleSize);
      RoleHistoryFooter footer = null;
      int records = 0;
      //go through reading data
      try { while (true) {
        record = reader.read(null, decoder);
        entry = record.getEntry();

        if (entry instanceof RoleHistoryHeader) {
          throw new IOException("Duplicate Role History Header found");
        }
        if (entry instanceof RoleHistoryFooter) {
          //tail end of the file
          footer = (RoleHistoryFooter) entry;
          break;
        }
        records++;
        NodeEntryRecord nodeEntryRecord = (NodeEntryRecord) entry;
        NodeEntry nodeEntry = new NodeEntry();
        nodeEntry.setLastUsed(nodeEntryRecord.getLastUsed());
        if (nodeEntryRecord.getActive()) {
          //if active at the time of save, make the last used time the save time
          nodeEntry.setLastUsed(saved);
        }
        Integer roleId = nodeEntryRecord.getRole();
        NodeAddress addr = nodeEntryRecord.getNode();
        NodeInstance instance = history.getOrCreateNodeInstance(addr);
        instance.set(roleId, nodeEntry);
      } } catch (EOFException e) {
        EOFException ex = new EOFException(
          "End of file reached after " + records + " records");
        ex.initCause(e);
        throw ex;
      }
      //at this point there should be no data left. 
      if (in.read() > 0) {
        // footer is in stream before the last record
        throw new EOFException(
          "File footer reached before end of file -after " + records +
          " records");
      }
      if (records != footer.getCount()) {
        log.warn("mismatch between no of records saved {} and number read {}",
                 footer.getCount(), records);
      }
      return records;
    } finally {
      in.close();
    }

  }

  public int read(FileSystem fs, Path path, RoleHistory roleHistory) throws
                                                                     IOException {
    FSDataInputStream instream = fs.open(path);
    return read(instream, roleHistory);
  }

  public int read(File file, RoleHistory roleHistory) throws
                                                      IOException {


    
    return read(new FileInputStream(file), roleHistory);
  }
  
  public int read(String resource, RoleHistory roleHistory) throws
                                                      IOException {


    
    return read(this.getClass().getClassLoader().getResourceAsStream(resource), roleHistory);
  }
  
  
}
