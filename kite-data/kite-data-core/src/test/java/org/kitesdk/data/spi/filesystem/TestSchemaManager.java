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

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.junit.Assert;
import org.kitesdk.data.MiniDFSTest;

import java.io.IOException;


public class TestSchemaManager extends MiniDFSTest {

    Path testPath = new Path("/test_dir");

    @Test
    public void testCreateSchema() throws IOException {

        SchemaManager manager = SchemaManager.create(getConfiguration(), testPath);

        manager.writeSchema(DatasetTestUtilities.USER_SCHEMA);

        Schema schema = manager.getNewestSchema();

        Assert.assertEquals(DatasetTestUtilities.USER_SCHEMA, schema);
    }

    @Test
    public void testUpdateSchema() throws IOException {

        SchemaManager manager = SchemaManager.create(getConfiguration(), testPath);

        manager.writeSchema(DatasetTestUtilities.USER_SCHEMA);

        Schema schema = manager.getNewestSchema();

        Assert.assertEquals(DatasetTestUtilities.USER_SCHEMA, schema);

        // Create an updated schema and ensure it can be written.
        Schema updatedSchema = SchemaBuilder.record(schema.getName())
                .fields()
                .requiredString("username")
                .requiredString("email")
                .optionalBoolean("isAdmin").endRecord();

        manager.writeSchema(updatedSchema);

        Assert.assertEquals(updatedSchema, manager.getNewestSchema());
    }

}
