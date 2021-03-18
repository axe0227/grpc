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
#include <mutex>
#include <condition_variable>
#include <chrono>

#include <grpc++/grpc++.h>

#include "hellostreamingworld.grpc.pb.h"

using grpc::Channel;
using grpc::ClientAsyncReaderWriter;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;
using hellostreamingworld::HelloRequest;
using hellostreamingworld::HelloReply;
using hellostreamingworld::MultiGreeter;

//NOTE: This is a complex example for an asynchronous, bidirectional streaming
// client. For a simpler example, start with the
// greeter_client/greeter_async_client first.
class AsyncBidiGreeterClient {
    enum class Type {
        READ = 1,
        WRITE = 2,
        CONNECT = 3,
        WRITES_DONE = 4,
        FINISH = 5
    };

public:
    explicit AsyncBidiGreeterClient(std::shared_ptr<Channel> channel)
        : stub_(MultiGreeter::NewStub(channel)) {
        grpc_thread_.reset(
            new std::thread(std::bind(&AsyncBidiGreeterClient::GrpcThread, this)));
        stream_ = stub_->AsyncSayHello(&context_, &cq_,
            reinterpret_cast<void*>(Type::CONNECT));
        
        // sleep for a while to let the CONNECT request finish and cq received the tag
        // then we can start doing Write
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    ~AsyncBidiGreeterClient() {
        std::cout << "Shutting down client...." << std::endl;
        grpc::Status status;
        cq_.Shutdown();
        grpc_thread_->join();
    }

    // Similar to the async hello example in greeter_async_client but does not
    // wait for the response. Instead queues up a tag in the completion queue
    // that is notified when the server responds back (or when the stream is
    // closed). Returns false when the stream is requested to be closed.
    bool AsyncSayHello(const std::string& user) {

        rpc_counter_.fetch_add(1);
        // If it's not ready
        std::string message = user + std::to_string(rpc_counter_.load());
        {
            if(!ready_.load()){
                // std::cout << "[m] wait : " << message << std::endl;
                std::unique_lock<std::mutex> lk(mu_);
                // wait until 
                cv_.wait_for(lk, std::chrono::milliseconds(100), [&](){return ready_.load();});
                // std::cout << "[m] Notified \n";
                ready_.store(false);
            }
        }

        // if (user == "quit") {
        //     stream_->WritesDone(reinterpret_cast<void*>(Type::WRITES_DONE));
        //     return true;
        // }

        // Data we are sending to the server.
        request_.set_name(message);

        // This is important: You can have at most one write or at most one read
        // at any given time. The throttling is performed by gRPC completion
        // queue. If you queue more than one write/read, the stream will crash.
        // Because this stream is bidirectional, you *can* have a single read
        // and a single write request queued for the same stream. Writes and reads
        // are independent of each other in terms of ordering/delivery.
        //std::cout << " ** Sending request: " << user << std::endl;
        // std::cout << "[m] Write \n";
        stream_->Write(request_, reinterpret_cast<void*>(Type::WRITE));

        // Manual unlocking is done before notifying, to avoid waking up
        // the waiting thread only to block again (see notify_one for details)
        
        if(ready_.load()){
            ready_.store(false);
        }
        return true;
    }

private:
    void AsyncHelloRequestNextMessage() {

        // The tag is the link between our thread (main thread) and the completion
        // queue thread. The tag allows the completion queue to fan off
        // notification handlers for the specified read/write requests as they
        // are being processed by gRPC.
        stream_->Read(&response_, reinterpret_cast<void*>(Type::READ));
    }

    // Runs a gRPC completion-queue processing thread. Checks for 'Next' tag
    // and processes them until there are no more (or when the completion queue
    // is shutdown).
    void GrpcThread() {
        while (true) {
            void* got_tag;
            bool ok = false;
            // Block until the next result is available in the completion queue "cq".
            // The return value of Next should always be checked. This return value
            // tells us whether there is any kind of event or the cq_ is shutting
            // down.

            if (!cq_.Next(&got_tag, &ok)) {
                std::cerr << "Client stream closed. Quitting" << std::endl;
                break;
            }

            // It's important to process all tags even if the ok is false. One might
            // want to deallocate memory that has be reinterpret_cast'ed to void*
            // when the tag got initialized. For our example, we cast an int to a
            // void*, so we don't have extra memory management to take care of.
            if (ok) {
                //std::cout << std::endl << "**** Processing completion queue tag " << got_tag << std::endl;
                switch (static_cast<Type>(reinterpret_cast<long>(got_tag))) {
                case Type::READ:
                    std::cout << "Read a new message:" << response_.message() << std::endl;
                    break;
                case Type::WRITE:
                    // std::cout << "[g] polled :" << request_.name() << std::endl;
                    // received a tag on the cq, notify the main thread that we can start a new Write
                    
                    // if(!ready_.load()){
                        ready_.store(true);
                        // std::cout << "[g] Notifying \n";
                        cv_.notify_one();
                    // }
                
                    break;
                case Type::CONNECT:
                    std::cout << "Server connected." << std::endl;
                    break;
                case Type::WRITES_DONE:
                    std::cout << "writesdone sent,sleeping 5s" << std::endl;
                    stream_->Finish(&finish_status_, reinterpret_cast<void*>(Type::FINISH));
                    break;
                case Type::FINISH:
                    std::cout << "Client finish status:" << finish_status_.error_code() << ", msg:" << finish_status_.error_message() << std::endl;
                    //context_.TryCancel();
                    cq_.Shutdown();
                    break;
                default:
                    std::cerr << "Unexpected tag " << got_tag << std::endl;
                    // assert(false);
                    break;
                }
            }
        }
    }
    // is this the first time we call write
    std::atomic_bool ready_ {true};
    std::mutex mu_;
    std::condition_variable_any cv_;
    // Context for the client. It could be used to convey extra information to
    // the server and/or tweak certain RPC behaviors.
    ClientContext context_;

    // The producer-consumer queue we use to communicate asynchronously with the
    // gRPC runtime.
    CompletionQueue cq_;

    // Out of the passed in Channel comes the stub, stored here, our view of the
    // server's exposed services.
    std::unique_ptr<MultiGreeter::Stub> stub_;

    // The bidirectional, asynchronous stream for sending/receiving messages.
    std::unique_ptr<ClientAsyncReaderWriter<HelloRequest, HelloReply>> stream_;

    // Allocated protobuf that holds the response. In real clients and servers,
    // the memory management would a bit more complex as the thread that fills
    // in the response should take care of concurrency as well as memory
    // management.
    HelloRequest request_;
    HelloReply response_;

    // Thread that notifies the gRPC completion queue tags.
    std::unique_ptr<std::thread> grpc_thread_;

    // Finish status when the client is done with the stream.
    grpc::Status finish_status_ = grpc::Status::OK;

    std::atomic<uint64_t> rpc_counter_{0};
};

int main(int argc, char** argv) {
    AsyncBidiGreeterClient greeter(grpc::CreateChannel(
        "localhost:50051", grpc::InsecureChannelCredentials()));

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    int num;
    while (true) {
        std::cout << "Enter number of ops (type quit to end): ";
        std::cin >> num;
        // Async RPC call that sends a message and awaits a response.
        for(int i = 0; i < num; i++){
            if(!greeter.AsyncSayHello("hello")){
                std::cout << "Quitting." << std::endl;
                break;
            }
        }
        
    }
    return 0;
}