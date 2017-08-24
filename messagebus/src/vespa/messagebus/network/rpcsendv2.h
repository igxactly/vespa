// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
#pragma once

#include "rpcsendv1.h"

namespace mbus {

/**
 * Implements the send adapter for method "mbus.send".
 */
class RPCSendV2 : public RPCSendAdapter,
                  public FRT_Invokable,
                  public FRT_IRequestWait,
                  public IDiscardHandler,
                  public IReplyHandler {
private:
    RPCNetwork *_net;
    string _clientIdent;
    string _serverIdent;

    /**
     * Send an error reply for a given request.
     *
     * @param request    The FRT request to reply to.
     * @param version    The version to serialize for.
     * @param traceLevel The trace level to set in the reply.
     * @param err        The error to reply with.
     */
    void replyError(FRT_RPCRequest *req, const vespalib::Version &version,
                    uint32_t traceLevel, const Error &err);

    void send(RoutingNode &recipient, const vespalib::Version &version,
              const PayLoadFiller & filler, uint64_t timeRemaining);
public:
    /** The name of the rpc method that this adapter registers. */
    static const char *METHOD_NAME;

    /** The parameter string of the rpc method. */
    static const char *METHOD_PARAMS;

    /** The return string of the rpc method. */
    static const char *METHOD_RETURN;

    /**
     * Constructs a new instance of this adapter. This object is unusable until
     * its attach() method has been called.
     */
    RPCSendV2();
    ~RPCSendV2();

    void attach(RPCNetwork &net) override;

    void send(RoutingNode &recipient, const vespalib::Version &version,
              BlobRef payload, uint64_t timeRemaining) override;
    void sendByHandover(RoutingNode &recipient, const vespalib::Version &version,
              Blob payload, uint64_t timeRemaining) override;

    void handleReply(std::unique_ptr<Reply> reply) override;
    void handleDiscard(Context ctx) override;
    void invoke(FRT_RPCRequest *req);
    void RequestDone(FRT_RPCRequest *req) override;
};

} // namespace mbus
