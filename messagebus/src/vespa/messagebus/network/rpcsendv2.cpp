// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#include "rpcsendv2.h"
#include "rpcsend_private.h"
#include "rpcnetwork.h"
#include "rpcserviceaddress.h"
#include <vespa/messagebus/emptyreply.h>
#include <vespa/messagebus/errorcode.h>
#include <vespa/messagebus/tracelevel.h>
#include <vespa/vespalib/util/stringfmt.h>
#include <vespa/fnet/channel.h>

using vespalib::make_string;

namespace mbus {

using network::internal::SendContext;
using network::internal::ReplyContext;

namespace {

const char *METHOD_NAME   = "mbus.send1";
const char *METHOD_PARAMS = "sssbilsxi";
const char *METHOD_RETURN = "sdISSsxs";

}

bool RPCSendV2::isCompatible(vespalib::stringref method, vespalib::stringref request, vespalib::stringref respons)
{
    return  (method == METHOD_NAME) &&
            (request == METHOD_PARAMS) &&
            (respons == METHOD_RETURN);
}

void
RPCSendV2::build(FRT_ReflectionBuilder & builder)
{
    builder.DefineMethod(METHOD_NAME, METHOD_PARAMS, METHOD_RETURN, true, FRT_METHOD(RPCSendV2::invoke), this);
    builder.MethodDesc("Send a message bus request and get a reply back.");
    builder.ParamDesc("version", "The version of the message.");
    builder.ParamDesc("route", "Names of additional hops to visit.");
    builder.ParamDesc("session", "The local session that should receive this message.");
    builder.ParamDesc("retryEnabled", "Whether or not this message can be resent.");
    builder.ParamDesc("retry", "The number of times the sending of this message has been retried.");
    builder.ParamDesc("timeRemaining", "The number of milliseconds until timeout.");
    builder.ParamDesc("protocol", "The name of the protocol that knows how to decode this message.");
    builder.ParamDesc("payload", "The protocol specific message payload.");
    builder.ParamDesc("level", "The trace level of the message.");
    builder.ReturnDesc("version", "The lowest version the message was serialized as.");
    builder.ReturnDesc("retry", "The retry request of the reply.");
    builder.ReturnDesc("errorCodes", "The reply error codes.");
    builder.ReturnDesc("errorMessages", "The reply error messages.");
    builder.ReturnDesc("errorServices", "The reply error service names.");
    builder.ReturnDesc("protocol", "The name of the protocol that knows how to decode this reply.");
    builder.ReturnDesc("payload", "The protocol specific reply payload.");
    builder.ReturnDesc("trace", "A string representation of the trace.");
}

void
RPCSendV2::send(RoutingNode &recipient, const vespalib::Version &version,
                const PayLoadFiller & payload, uint64_t timeRemaining)
{
    SendContext::UP ctx(new SendContext(recipient, timeRemaining));
    RPCServiceAddress &address = static_cast<RPCServiceAddress&>(recipient.getServiceAddress());
    const Message &msg = recipient.getMessage();
    Route route = recipient.getRoute();
    Hop hop = route.removeHop(0);

    FRT_RPCRequest *req = _net->allocRequest();
    FRT_Values &args = *req->GetParams();
    req->SetMethodName(METHOD_NAME);
    args.AddString(version.toString().c_str());
    args.AddString(route.toString().c_str());
    args.AddString(address.getSessionName().c_str());
    args.AddInt8(msg.getRetryEnabled() ? 1 : 0);
    args.AddInt32(msg.getRetry());
    args.AddInt64(timeRemaining);
    args.AddString(msg.getProtocol().c_str());
    payload.fill(args);
    args.AddInt32(recipient.getTrace().getLevel());

    if (ctx->getTrace().shouldTrace(TraceLevel::SEND_RECEIVE)) {
        ctx->getTrace().trace(TraceLevel::SEND_RECEIVE,
                              make_string("Sending message (version %s) from %s to '%s' with %.2f seconds timeout.",
                                          version.toString().c_str(), _clientIdent.c_str(),
                                          address.getServiceName().c_str(), ctx->getTimeout()));
    }

    if (hop.getIgnoreResult()) {
        address.getTarget().getFRTTarget().InvokeVoid(req);
        if (ctx->getTrace().shouldTrace(TraceLevel::SEND_RECEIVE)) {
            ctx->getTrace().trace(TraceLevel::SEND_RECEIVE,
                                  make_string("Not waiting for a reply from '%s'.", address.getServiceName().c_str()));
        }
        Reply::UP reply(new EmptyReply());
        reply->getTrace().swap(ctx->getTrace());
        _net->getOwner().deliverReply(std::move(reply), recipient);
    } else {
        SendContext *ptr = ctx.release();
        req->SetContext(FNET_Context(ptr));
        address.getTarget().getFRTTarget().InvokeAsync(req, ptr->getTimeout(), this);
    }
}

void
RPCSendV2::RequestDone(FRT_RPCRequest *req)
{
    SendContext::UP ctx(static_cast<SendContext*>(req->GetContext()._value.VOIDP));
    const string &serviceName = static_cast<RPCServiceAddress&>(
            ctx->getRecipient().getServiceAddress()).getServiceName();
    Reply::UP reply;
    Error error;
    if (!req->CheckReturnTypes(METHOD_RETURN)) {
        reply.reset(new EmptyReply());
        switch (req->GetErrorCode()) {
        case FRTE_RPC_TIMEOUT:
            error = Error(ErrorCode::TIMEOUT,
                          make_string("A timeout occured while waiting for '%s' (%g seconds expired); %s",
                                      serviceName.c_str(), ctx->getTimeout(), req->GetErrorMessage()));
            break;
        case FRTE_RPC_CONNECTION:
            error = Error(ErrorCode::CONNECTION_ERROR,
                          make_string("A connection error occured for '%s'; %s",
                                      serviceName.c_str(), req->GetErrorMessage()));
            break;
        default:
            error = Error(ErrorCode::NETWORK_ERROR,
                          make_string("A network error occured for '%s'; %s",
                                      serviceName.c_str(), req->GetErrorMessage()));
        }
    } else {
        FRT_Values &ret = *req->GetReturn();

        vespalib::Version version          = vespalib::Version(ret[0]._string._str);
        double            retryDelay       = ret[1]._double;
        uint32_t         *errorCodes       = ret[2]._int32_array._pt;
        uint32_t          errorCodesLen    = ret[2]._int32_array._len;
        FRT_StringValue  *errorMessages    = ret[3]._string_array._pt;
        uint32_t          errorMessagesLen = ret[3]._string_array._len;
        FRT_StringValue  *errorServices    = ret[4]._string_array._pt;
        uint32_t          errorServicesLen = ret[4]._string_array._len;
        const char       *protocolName     = ret[5]._string._str;
        const char       *payload          = ret[6]._data._buf;
        uint32_t          payloadLen       = ret[6]._data._len;
        const char       *trace            = ret[7]._string._str;

        if (payloadLen > 0) {
            IProtocol * protocol = _net->getOwner().getProtocol(protocolName);
            if (protocol != nullptr) {
                Routable::UP routable = protocol->decode(version, BlobRef(payload, payloadLen));
                if (routable) {
                    if (routable->isReply()) {
                        reply.reset(static_cast<Reply*>(routable.release()));
                    } else {
                        error = Error(ErrorCode::DECODE_ERROR,
                                      "Payload decoded to a message when expecting a reply.");
                    }
                } else {
                    error = Error(ErrorCode::DECODE_ERROR,
                                  make_string("Protocol '%s' failed to decode routable.", protocolName));
                }

            } else {
                error = Error(ErrorCode::UNKNOWN_PROTOCOL,
                              make_string("Protocol '%s' is not known by %s.", protocolName, _serverIdent.c_str()));
            }
        }
        if ( ! reply ) {
            reply.reset(new EmptyReply());
        }
        reply->setRetryDelay(retryDelay);
        for (uint32_t i = 0; i < errorCodesLen && i < errorMessagesLen && i < errorServicesLen; ++i) {
            reply->addError(Error(errorCodes[i],
                                  errorMessages[i]._str,
                                  errorServices[i]._len > 0 ? errorServices[i]._str : serviceName.c_str()));
        }
        ctx->getTrace().getRoot().addChild(TraceNode::decode(trace));
    }
    if (ctx->getTrace().shouldTrace(TraceLevel::SEND_RECEIVE)) {
        ctx->getTrace().trace(TraceLevel::SEND_RECEIVE,
                              make_string("Reply (type %d) received at %s.", reply->getType(), _clientIdent.c_str()));
    }
    reply->getTrace().swap(ctx->getTrace());
    if (error.getCode() != ErrorCode::NONE) {
        reply->addError(error);
    }
    _net->getOwner().deliverReply(std::move(reply), ctx->getRecipient());
    req->SubRef();
}

void
RPCSendV2::invoke(FRT_RPCRequest *req)
{
    req->Detach();

    FRT_Values &args = *req->GetParams();
    vespalib::Version  version       = vespalib::Version(args[0]._string._str);
    const char        *route         = args[1]._string._str;
    const char        *session       = args[2]._string._str;
    bool               retryEnabled  = args[3]._intval8 != 0;
    uint32_t           retry         = args[4]._intval32;
    uint64_t           timeRemaining = args[5]._intval64;
    const char        *protocolName  = args[6]._string._str;
    const char        *payload       = args[7]._data._buf;
    uint32_t           payloadLen    = args[7]._data._len;
    uint32_t           traceLevel    = args[8]._intval32;

    IProtocol * protocol = _net->getOwner().getProtocol(protocolName);
    if (protocol == nullptr) {
        replyError(req, version, traceLevel,
                   Error(ErrorCode::UNKNOWN_PROTOCOL,
                         make_string("Protocol '%s' is not known by %s.", protocolName, _serverIdent.c_str())));
        return;
    }
    Routable::UP routable = protocol->decode(version, BlobRef(payload, payloadLen));
    req->DiscardBlobs();
    if ( ! routable ) {
        replyError(req, version, traceLevel,
                   Error(ErrorCode::DECODE_ERROR,
                         make_string("Protocol '%s' failed to decode routable.", protocolName)));
        return;
    }
    if (routable->isReply()) {
        replyError(req, version, traceLevel,
                   Error(ErrorCode::DECODE_ERROR,
                         "Payload decoded to a reply when expecting a mesage."));
        return;
    }
    Message::UP msg(static_cast<Message*>(routable.release()));
    if (strlen(route) > 0) {
        msg->setRoute(Route::parse(route));
    }
    msg->setContext(Context(new ReplyContext(*req, version)));
    msg->pushHandler(*this, *this);
    msg->setRetryEnabled(retryEnabled);
    msg->setRetry(retry);
    msg->setTimeReceivedNow();
    msg->setTimeRemaining(timeRemaining);
    msg->getTrace().setLevel(traceLevel);
    if (msg->getTrace().shouldTrace(TraceLevel::SEND_RECEIVE)) {
        msg->getTrace().trace(TraceLevel::SEND_RECEIVE,
                              make_string("Message (type %d) received at %s for session '%s'.",
                                          msg->getType(), _serverIdent.c_str(), session));
    }
    _net->getOwner().deliverMessage(std::move(msg), session);
}

void
RPCSendV2::handleReply(Reply::UP reply)
{
    ReplyContext::UP ctx(static_cast<ReplyContext*>(reply->getContext().value.PTR));
    FRT_RPCRequest &req = ctx->getRequest();
    string version = ctx->getVersion().toString();
    if (reply->getTrace().shouldTrace(TraceLevel::SEND_RECEIVE)) {
        reply->getTrace().trace(TraceLevel::SEND_RECEIVE, make_string("Sending reply (version %s) from %s.",
                                                                      version.c_str(), _serverIdent.c_str()));
    }
    Blob payload(0);
    if (reply->getType() != 0) {
        payload = _net->getOwner().getProtocol(reply->getProtocol())->encode(ctx->getVersion(), *reply);
        if (payload.size() == 0) {
            reply->addError(Error(ErrorCode::ENCODE_ERROR, "An error occured while encoding the reply, see log."));
        }
    }
    FRT_Values &ret = *req.GetReturn();
    ret.AddString(version.c_str());
    ret.AddDouble(reply->getRetryDelay());

    uint32_t         errorCount    = reply->getNumErrors();
    uint32_t        *errorCodes    = ret.AddInt32Array(errorCount);
    FRT_StringValue *errorMessages = ret.AddStringArray(errorCount);
    FRT_StringValue *errorServices = ret.AddStringArray(errorCount);
    for (uint32_t i = 0; i < errorCount; ++i) {
        errorCodes[i] = reply->getError(i).getCode();
        ret.SetString(errorMessages + i, reply->getError(i).getMessage().c_str());
        ret.SetString(errorServices + i, reply->getError(i).getService().c_str());
    }

    ret.AddString(reply->getProtocol().c_str());
    ret.AddData(std::move(payload.payload()), payload.size());
    if (reply->getTrace().getLevel() > 0) {
        ret.AddString(reply->getTrace().getRoot().encode().c_str());
    } else {
        ret.AddString("");
    }
    req.Return();
}


} // namespace mbus
