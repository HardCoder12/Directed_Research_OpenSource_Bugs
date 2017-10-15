/**
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
package org.apache.hadoop.hdfs;

import java.io.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.io.IOUtils;

public class TestLease extends junit.framework.TestCase {
    
  private static Log LOG = LogFactory.getLog(TestLease.class);
    
  static boolean hasLease(MiniDFSCluster cluster, Path src) {
    return cluster.getNamesystem().leaseManager.getLeaseByPath(src.toString()) != null;
  }
  
  final Path dir = new Path("/test/lease/");
    
  @Test
   public void testSaveNamespaceWithRenamedLease() throws Exception {
     MiniDFSCluster cluster = new MiniDFSCluster.Builder(new Configuration())
       .numDataNodes(1)
       .build();
     cluster.waitActive();
     DistributedFileSystem fs = (DistributedFileSystem) cluster.getFileSystem();
     OutputStream out = null;
     try {
       fs.mkdirs(new Path("/test-target"));
       out = fs.create(new Path("/test-source/foo")); // don't close
       fs.rename(new Path("/test-source/"), new Path("/test-target/"));
       
       fs.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
       cluster.getNameNodeRpc().saveNamespace();
       fs.setSafeMode(SafeModeAction.SAFEMODE_LEAVE);
     } finally {
       IOUtils.cleanup(LOG, out, fs);
       if (cluster != null) {
         cluster.shutdown();
       }
     }
   }    

  public void testLease() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 2, true, null);
    try {
      FileSystem fs = cluster.getFileSystem();
      assertTrue(fs.mkdirs(dir));
      
      
      Path a = new Path(dir, "a");
      Path b = new Path(dir, "b");

      DataOutputStream a_out = fs.create(a);
      a_out.writeBytes("something");

      assertTrue(hasLease(cluster, a));
      assertTrue(!hasLease(cluster, b));
      
      DataOutputStream b_out = fs.create(b);
      b_out.writeBytes("something");

      assertTrue(hasLease(cluster, a));
      assertTrue(hasLease(cluster, b));

      a_out.close();
      b_out.close();

      assertTrue(!hasLease(cluster, a));
      assertTrue(!hasLease(cluster, b));
      
      fs.delete(dir, true);
    } finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }
}
