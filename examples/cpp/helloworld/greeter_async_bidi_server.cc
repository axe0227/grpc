/*
 *
 * Copyright 2016, Google Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *     * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *     * Neither the name of Google Inc. nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <random>
#include <atomic>

#include <grpc++/grpc++.h>
#include "hellostreamingworld.grpc.pb.h"

using grpc::Server;
using grpc::ServerAsyncReaderWriter;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerCompletionQueue;
using grpc::Status;
using grpc::StatusCode;
using hellostreamingworld::HelloRequest;
using hellostreamingworld::HelloReply;
using hellostreamingworld::MultiGreeter;

int g_thread_num = 1;
int g_cq_num = 1;
int g_pool = 1;
int g_port = 50051;

std::atomic<void*>**    g_instance_pool = nullptr;

class CallDataBase {
public:
  CallDataBase(MultiGreeter::AsyncService* service, ServerCompletionQueue* cq)
   :service_(service), cq_(cq){}

  virtual void Proceed(bool ok) = 0;

protected:

  // The means of communication with the gRPC runtime for an asynchronous
  // server.
  MultiGreeter::AsyncService* service_;
  // The producer-consumer queue where for asynchronous server notifications.
  ServerCompletionQueue* cq_;

  // Context for the rpc, allowing to tweak aspects of it such as the use
  // of compression, authentication, as well as to send metadata back to the
  // client.
  ServerContext ctx_;

  // What we get from the client.
  HelloRequest request_;
  // What we send back to the client.
  HelloReply reply_;
};

class CallDataBidi : CallDataBase {

 public:

  // Take in the "service" instance (in this case representing an asynchronous
  // server) and the completion queue "cq" used for asynchronous communication
  // with the gRPC runtime.
  CallDataBidi(MultiGreeter::AsyncService* service, ServerCompletionQueue* cq)
   :CallDataBase(service,cq),rw_(&ctx_){
    // Invoke the serving logic right away.

    status_ = BidiStatus::CONNECT;

    ctx_.AsyncNotifyWhenDone((void*)this);
    service_->RequestSayHello(&ctx_, &rw_, cq_, cq_, (void*)this);
  }

  void Proceed(bool ok) {

    std::unique_lock<std::mutex> _wlock(this->m_mutex);

    switch (status_) {
    case BidiStatus::READ:

        //Meaning client said it wants to end the stream either by a 'writedone' or 'finish' call.
        if (!ok) {
            std::cout << "thread:" << std::this_thread::get_id() << " tag:" << this << " CQ returned false." << std::endl;
            Status _st(StatusCode::OUT_OF_RANGE,"test error msg");
            rw_.Write(reply_, (void*)this);

            rw_.Finish(_st,(void*)this);
            status_ = BidiStatus::DONE;
            std::cout << "thread:" << std::this_thread::get_id() << " tag:" << this << " after call Finish(), cancelled:" << this->ctx_.IsCancelled() << std::endl;
            break;
        }

        std::cout << "thread:" << std::this_thread::get_id() << " tag:" << this << " Read a new message:" << request_.name() << std::endl;

        reply_.set_message("arthur");
        rw_.Read(&request_, (void*)this);

        status_ = BidiStatus::READ;
        break;

    case BidiStatus::WRITE:
        std::cout << "thread:" << std::this_thread::get_id() << " tag:" << this << " Written a message:" << reply_.message() << std::endl;
        rw_.Read(&request_, (void*)this);
        status_ = BidiStatus::WRITE;
        break;

    case BidiStatus::CONNECT:
        std::cout << "thread:" << std::this_thread::get_id() << " tag:" << this << " connected:" << std::endl;
        new CallDataBidi(service_, cq_);
        rw_.Read(&request_, (void*)this);
        status_ = BidiStatus::READ;
        break;

    case BidiStatus::DONE:
        std::cout << "thread:" << std::this_thread::get_id() << " tag:" << this
                << " Server done, cancelled:" << this->ctx_.IsCancelled() << std::endl;
        status_ = BidiStatus::FINISH;
        break;

    case BidiStatus::FINISH:
        std::cout << "thread:" << std::this_thread::get_id() <<  "tag:" << this << " Server finish, cancelled:" << this->ctx_.IsCancelled() << std::endl;
        _wlock.unlock();
        delete this;
        break;

    default:
        std::cerr << "Unexpected tag " << int(status_) << std::endl;
        assert(false);
    }
  }

 private:

  // The means to get back to the client.
  ServerAsyncReaderWriter<HelloReply, HelloRequest>  rw_;

  // Let's implement a tiny state machine with the following states.
  enum class BidiStatus { READ = 1, WRITE = 2, CONNECT = 3, DONE = 4, FINISH = 5 };
  BidiStatus status_;

  std::mutex    m_mutex;
};


class ServerImpl final {
 public:
  ~ServerImpl() {
    server_->Shutdown();
    // Always shutdown the completion queue after the server.
    for (const auto& _cq : m_cq)
        _cq->Shutdown();
  }

  // There is no shutdown handling in this code.
  void Run() {
    std::string server_address("0.0.0.0:" + std::to_string(g_port));

    ServerBuilder builder;
    // Listen on the given address without any authentication mechanism.
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    // Register "service_" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *asynchronous* service.
    builder.RegisterService(&service_);
    // Get hold of the completion queue used for the asynchronous communication
    // with the gRPC runtime.

    for (int i = 0; i < g_cq_num; ++i) {
        //cq_ = builder.AddCompletionQueue();
        m_cq.emplace_back(builder.AddCompletionQueue());
    }

    // Finally assemble the server.
    server_ = builder.BuildAndStart();
    std::cout << "Server listening on " << server_address << std::endl;

    // Proceed to the server's main loop.
    std::vector<std::thread*> _vec_threads;

    for (int i = 0; i < g_thread_num; ++i) {
        int _cq_idx = i % g_cq_num;
        for (int j = 0; j < g_pool; ++j) {
            // new CallDataUnary(&service_, m_cq[_cq_idx].get());
            new CallDataBidi(&service_, m_cq[_cq_idx].get());
        }

        _vec_threads.emplace_back(new std::thread(&ServerImpl::HandleRpcs, this, _cq_idx));
    }

    std::cout << g_thread_num << " working aysnc threads spawned" << std::endl;

    for (const auto& _t : _vec_threads)
        _t->join();
  }

 private:
  // Class encompasing the state and logic needed to serve a request.

  // This can be run in multiple threads if needed.
  void HandleRpcs(int cq_idx) {
    // Spawn a new CallDataUnary instance to serve new clients.
    void* tag;  // uniquely identifies a request.
    bool ok;
    while (true) {
      // Block waiting to read the next event from the completion queue. The
      // event is uniquely identified by its tag, which in this case is the
      // memory address of a CallDataUnary instance.
      // The return value of Next should always be checked. This return value
      // tells us whether there is any kind of event or cq_ is shutting down.
      //GPR_ASSERT(cq_->Next(&tag, &ok));
      GPR_ASSERT(m_cq[cq_idx]->Next(&tag, &ok));

      CallDataBase* _p_ins = (CallDataBase*)tag;
      _p_ins->Proceed(ok);
    }
  }

  std::vector<std::unique_ptr<ServerCompletionQueue>>  m_cq;

  MultiGreeter::AsyncService service_;
  std::unique_ptr<Server> server_;
};

const char* ParseCmdPara( char* argv,const char* para) {
    auto p_target = std::strstr(argv,para);
    if (p_target == nullptr) {
        printf("para error argv[%s] should be %s \n",argv,para);
        return nullptr;
    }
    p_target += std::strlen(para);
    return p_target;
}

int main(int argc, char** argv) {
  // if (argc != 5) {
  //     std::cout << "Usage:./program --thread=xx --cq=xx --pool=xx --port=xx";
  //     return 0;
  // }

  // g_thread_num = std::atoi(ParseCmdPara(argv[1],"--thread="));
  // g_cq_num = std::atoi(ParseCmdPara(argv[2],"--cq="));
  // g_pool = std::atoi(ParseCmdPara(argv[3],"--pool="));
  // g_port = std::atoi(ParseCmdPara(argv[4],"--port="));

  ServerImpl server;
  server.Run();

  return 0;
}