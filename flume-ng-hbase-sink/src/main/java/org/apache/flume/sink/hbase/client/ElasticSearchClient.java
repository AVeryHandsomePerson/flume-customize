/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.sink.hbase.client;

import org.elasticsearch.client.Client;

/**
 * Interface for an ElasticSearch client which is responsible for sending bulks
 * of events to ElasticSearch.
 */
public interface ElasticSearchClient {
    /**
     * ElasticSearch
     *  Client
     * */
    public Client addClient(String clusterName, String ip, String post);
    /**
     * Index 大小转换
     * */
    public String convert(String index);
    /**
     *Index 是否存在判断
     * */
    public boolean judge(Client client, String index);
}
