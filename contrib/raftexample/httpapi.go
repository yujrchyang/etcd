// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"io/ioutil"
	"log"
	"net/http"
	"strconv"

	"go.etcd.io/etcd/raft/v3/raftpb"
)

// Handler for a http based key-value store backed by raft
// 该示例向外提供的是 HTTP 接口，用户可以通过调用 HTTP 接口来模拟客户端的行为
type httpKVAPI struct {
	// 持久化存储，用于保存用户提交的键值对信息
	store *kvstore
	// 当用户发送 POST（或 DELETE）请求时，会被认为是发送了一个集群节点增加（或删除）
	// 的请求，httpKVAPI 会将请求的信息写入 confChangeC 通道
	confChangeC chan<- raftpb.ConfChange
}

func (h *httpKVAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// 获取请求的 URL 作为 Key
	key := r.RequestURI
	defer r.Body.Close()
	switch {
	case r.Method == "PUT":
		// 读取 http 请求体
		v, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Printf("Failed to read on PUT (%v)\n", err)
			http.Error(w, "Failed on PUT", http.StatusBadRequest)
			return
		}

		// 对键值对进行序列化，之后将结果写入 proposeC 通道，后续 raftNode 会读取
		// 其中的数据进行处理
		h.store.Propose(key, string(v))

		// Optimistic-- no waiting for ack from raft. Value is not yet
		// committed so a subsequent GET on the key may return old value
		// 向用户返回相应的状态码
		w.WriteHeader(http.StatusNoContent)
	case r.Method == "GET":
		// 直接从 kvstore 中读取指定的键值对数据并返回给用户
		if v, ok := h.store.Lookup(key); ok {
			w.Write([]byte(v))
		} else {
			http.Error(w, "Failed to GET", http.StatusNotFound)
		}
	case r.Method == "POST":
		// 读取请求体，获取新加节点的 URL
		url, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Printf("Failed to read on POST (%v)\n", err)
			http.Error(w, "Failed on POST", http.StatusBadRequest)
			return
		}
		// 解析 URL 得到新增节点的 ID
		nodeId, err := strconv.ParseUint(key[1:], 0, 64)
		if err != nil {
			log.Printf("Failed to convert ID for conf change (%v)\n", err)
			http.Error(w, "Failed on POST", http.StatusBadRequest)
			return
		}

		cc := raftpb.ConfChange{
			Type:    raftpb.ConfChangeAddNode, // ConfChangeAddNode 即表示新增节点
			NodeID:  nodeId,                   // 指定新增节点的 ID
			Context: url,                      // 指定新增节点的 URL
		}
		// 将 ConfChange 实例写入 confChangeC 通道中
		h.confChangeC <- cc

		// As above, optimistic that raft will apply the conf change
		w.WriteHeader(http.StatusNoContent)
	case r.Method == "DELETE":
		nodeId, err := strconv.ParseUint(key[1:], 0, 64)
		if err != nil {
			log.Printf("Failed to convert ID for conf change (%v)\n", err)
			http.Error(w, "Failed on DELETE", http.StatusBadRequest)
			return
		}

		cc := raftpb.ConfChange{
			Type:   raftpb.ConfChangeRemoveNode, // ConfChangeAddNode 即表示新增节点
			NodeID: nodeId,                      // 指定待删除节点的 ID
		}
		// 将 ConfChange 实例写入 confChangeC 通道中
		h.confChangeC <- cc

		// As above, optimistic that raft will apply the conf change
		w.WriteHeader(http.StatusNoContent)
	default:
		w.Header().Set("Allow", "PUT")
		w.Header().Add("Allow", "GET")
		w.Header().Add("Allow", "POST")
		w.Header().Add("Allow", "DELETE")
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// serveHttpKVAPI starts a key-value server with a GET/PUT API and listens.
func serveHttpKVAPI(kv *kvstore, port int, confChangeC chan<- raftpb.ConfChange, errorC <-chan error) {
	// 创建 http.Server 用于接收 http 请求
	srv := http.Server{
		Addr: ":" + strconv.Itoa(port),
		// 设置 http.Server 的 Handler 字段，即上述 httpKVAPI 实例
		Handler: &httpKVAPI{
			store:       kv,
			confChangeC: confChangeC,
		},
	}

	// 启动单独的 goroutine 来监听 Addr 指定的地址，当有 HTTP 请求时，http.Server 会
	// 创建对应的 goroutine，并调用 httpKVAPI.ServeHTTP() 方法进行处理
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Fatal(err)
		}
	}()

	// exit when raft goes down
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}
