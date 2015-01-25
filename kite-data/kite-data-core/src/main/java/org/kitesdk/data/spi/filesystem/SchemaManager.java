/**
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kitesdk.data.spi.filesystem;

import com.google.common.base.Charsets;
import com.google.common.io.Closeables;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.kitesdk.data.DatasetIOException;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

/**
 * Manager for creating, updating, and using Avro schemas stored in a filesystem, typically HDFS.
 *
 * This is in progress. The final implementation should perform schema passivity checks when doing updates,
 * and possible allow the enumeration of historical schema versions for tooling.
 *
 */
public class SchemaManager {

  private final Path schemaDirectory;
  private final FileSystem rootFileSystem;

  /**
   * Creates a new schema manager using the given root directory of a dataset for its base.
   */
  public static SchemaManager create(Configuration conf, Path schemaDirectory) {

    try {
      FileSystem rootFileSystem = schemaDirectory.getFileSystem(conf);

      rootFileSystem.mkdirs(schemaDirectory);

      return new SchemaManager(schemaDirectory, rootFileSystem);
    } catch (IOException e) {
      throw new DatasetIOException("Unable to create schema manager directory: " + schemaDirectory, e);
    }

  }

  /**
   * Loads a schema manager that stores data under the given dataset root directory.
   */
  public static SchemaManager load(Configuration conf, Path schemaDirectory) {

    try {

      FileSystem rootFileSystem = schemaDirectory.getFileSystem(conf);

      if (rootFileSystem.exists(schemaDirectory)) {
        return new SchemaManager(schemaDirectory, rootFileSystem);
      } else {
        return null;
      }

    } catch (IOException e) {
      throw new DatasetIOException ("Cannot load schema manager at:"  + schemaDirectory, e);
    }
  }

  private SchemaManager(Path schemaDirectory, FileSystem rootFileSystem) throws IOException  {
    this.schemaDirectory = schemaDirectory;
    this.rootFileSystem = rootFileSystem;
  }

  /**
   * Returns the path of the newest schema file, or null if none exists.
   */
  private Path newestFile() {


    Path newestFile = null;

    try {
      for (FileStatus status : rootFileSystem.listStatus(schemaDirectory)) {

        // TODO: should use numeric comparison
        if (newestFile == null || newestFile.toString().compareTo(status.getPath().toString()) < 1) {
          newestFile = status.getPath();
        }

      }
    } catch (IOException e) {
      throw new DatasetIOException("Unable to list schema files.", e);
    }

    return newestFile;
  }

  /**
   * Returns the URI of the newest schema in the manager.
   */
  public URI getNewestSchemaURI() {

    Path path = newestFile();

    return path == null ? null : rootFileSystem.makeQualified(path).toUri();
  }

  private Schema loadSchema(Path schemaPath) {
    Schema schema = null;
    InputStream inputStream = null;
    boolean threw = true;

    try {
      inputStream = rootFileSystem.open(schemaPath);
      schema = new Schema.Parser().parse(inputStream);
      threw = false;
    } catch (IOException e) {
      throw new DatasetIOException(
              "Unable to load schema file:" + schemaPath, e);
    } finally {
      try {
        Closeables.close(inputStream, threw);
      } catch (IOException e) {
        throw new DatasetIOException("Cannot close", e);
      }
    }

    return schema;
  }

  /**
   * Returns the newest schema version being managed.
   */
  public Schema getNewestSchema() {
    Path schemaPath = newestFile();

    return schemaPath == null ? null : loadSchema(schemaPath);
  }

  /**
   * Imports an existing schema stored at the given path. This
   * is generally used to bring in schemas written by previous
   * versions of this library.
   */
  public URI importSchema(Path schemaPath) {

    Schema schema = loadSchema(schemaPath);

    return writeSchema(schema);
  }

  /**
   * Writes the schema and a URI to that schema.
   */
  public URI writeSchema(Schema schema) {

    Path previous = newestFile();

    Path schemaPath = null;

    if (previous == null) {
      schemaPath = new Path(schemaDirectory, "1.avsc");
    } else {

      String previousName = previous.getName().substring(0, previous.getName().indexOf('.'));

      int i = Integer.parseInt(previousName) + 1;

      schemaPath = new Path(schemaDirectory, Integer.toString(i) + ".avsc");
    }

    FSDataOutputStream outputStream = null;
    boolean threw = true;
    try {
      outputStream = rootFileSystem.create(schemaPath, false);
      outputStream.write(schema.toString(true)
              .getBytes(Charsets.UTF_8));
      outputStream.flush();
      threw = false;
    } catch (IOException e) {
      throw new DatasetIOException(
              "Unable to save schema file: " + schemaPath, e);
    } finally {
      try {
        Closeables.close(outputStream, threw);
      } catch (IOException e) {
        throw new DatasetIOException("Cannot close", e);
      }
    }

    return schemaPath.toUri();
  }
}
