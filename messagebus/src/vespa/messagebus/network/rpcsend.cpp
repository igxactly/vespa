// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#include "rpcsend.h"
#include "rpcsend_private.h"
#include <vespa/messagebus/network/rpcnetwork.h>
#include <vespa/messagebus/emptyreply.h>
#include <vespa/vespalib/util/stringfmt.h>
#include <vespa/fnet/channel.h>

using vespalib::make_string;

namespace mbus {

using network::internal::ReplyContext;

namespace {

class FillByCopy final : public PayLoadFiller
{
public:
    FillByCopy(BlobRef payload) : _payload(payload) { }
    void fill(FRT_Values & v) const override {
        v.AddData(_payload.data(), _payload.size());
    }
private:
    BlobRef _payload;
};

class FillByHandover final : public PayLoadFiller
{
public:
    FillByHandover(Blob payload) : _payload(std::move(payload)) { }
    void fill(FRT_Values & v) const override {
        v.AddData(std::move(_payload.payload()), _payload.size());
    }
private:
    mutable Blob _payload;
};

}

RPCSend::RPCSend() :
    _net(NULL),
    _clientIdent("client"),
    _serverIdent("server")
{ }

RPCSend::~RPCSend() {}

void
RPCSend::attach(RPCNetwork &net)
{
    _net = &net;
    const string &prefix = _net->getIdentity().getServicePrefix();
    if (!prefix.empty()) {
        _clientIdent = make_string("'%s'", prefix.c_str());
        _serverIdent = _clientIdent;
    }

    FRT_ReflectionBuilder builder(&_net->getSupervisor());
    build(builder);
}

void
RPCSend::replyError(FRT_RPCRequest *req, const vespalib::Version &version, uint32_t traceLevel, const Error &err)
{
    Reply::UP reply(new EmptyReply());
    reply->setContext(Context(new ReplyContext(*req, version)));
    reply->getTrace().setLevel(traceLevel);
    reply->addError(err);
    handleReply(std::move(reply));
}

void
RPCSend::handleDiscard(Context ctx)
{
    ReplyContext::UP tmp(static_cast<ReplyContext*>(ctx.value.PTR));
    FRT_RPCRequest &req = tmp->getRequest();
    FNET_Channel *chn = req.GetContext()._value.CHANNEL;
    req.SubRef();
    chn->Free();
}

void
RPCSend::sendByHandover(RoutingNode &recipient, const vespalib::Version &version,
                        Blob payload, uint64_t timeRemaining)
{
    send(recipient, version, FillByHandover(std::move(payload)), timeRemaining);
}

void
RPCSend::send(RoutingNode &recipient, const vespalib::Version &version,
                BlobRef payload, uint64_t timeRemaining)
{
    send(recipient, version, FillByCopy(payload), timeRemaining);
}

} // namespace mbus
