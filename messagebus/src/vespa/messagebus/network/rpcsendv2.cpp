// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#include "rpcsendv2.h"
#include "rpcsend_private.h"
#include "rpcnetwork.h"
#include "rpcserviceaddress.h"
#include <vespa/messagebus/emptyreply.h>
#include <vespa/messagebus/errorcode.h>
#include <vespa/messagebus/tracelevel.h>
#include <vespa/vespalib/util/stringfmt.h>
#include <vespa/vespalib/data/slime/slime.h>
#include <vespa/vespalib/data/databuffer.h>

using vespalib::make_string;

namespace mbus {

using network::internal::SendContext;
using network::internal::ReplyContext;

namespace {

const char *METHOD_NAME   = "mbus.slime";
const char *METHOD_PARAMS = "bxi";
const char *METHOD_RETURN = "bxi";

vespalib::Memory VERSION_F("version");
vespalib::Memory ROUTE_F("route");
vespalib::Memory SESSION_F("session");
vespalib::Memory USERETRY_F("use_retry");
vespalib::Memory RETRY_F("retry");
vespalib::Memory TIMELEFT_F("timeleft");
vespalib::Memory PROTOCOL_F("prot");
vespalib::Memory TRACELEVEL_F("trace");
vespalib::Memory BLOB_F("msg");

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
    builder.MethodDesc("Send a message bus slime request and get a reply back.");
    builder.ParamDesc("encoding", "0=raw, 6=lz4");
    builder.ParamDesc("uncompressedBlobSize", "Uncompressed blob size");
    builder.ParamDesc("message", "The message blob in slime");
    builder.ReturnDesc("encoding",  "0=raw, 6=lz4");
    builder.ReturnDesc("uncompressedBlobSize", "Uncompressed blob size");
    builder.ReturnDesc("reply", "The reply blob in slime.");
}

const char *
RPCSendV2::getReturnSpec() const {
    return METHOD_RETURN;
}

namespace {
class OutputBuf : public vespalib::Output {
public:
    OutputBuf(size_t estimatedSize) : _buf(estimatedSize) { }
    vespalib::DataBuffer & getBuf() { return _buf; }
private:
    vespalib::WritableMemory reserve(size_t bytes) override {
        _buf.ensureFree(bytes);
        return vespalib::WritableMemory(_buf.getFree(), _buf.getFreeLen());
    }
    Output &commit(size_t bytes) override {
        _buf.moveFreeToData(bytes);
        return *this;
    }
    vespalib::DataBuffer _buf;
};
}

void
RPCSendV2::encodeRequest(FRT_RPCRequest &req, const vespalib::Version &version, const Route & route,
                         const RPCServiceAddress & address, const Message & msg, uint32_t traceLevel,
                         const PayLoadFiller &filler, uint64_t timeRemaining) const
{
    FRT_Values &args = *req.GetParams();
    req.SetMethodName(METHOD_NAME);

    vespalib::Slime slime;
    vespalib::slime::Cursor & root = slime.get();

    root.setString(VERSION_F, version.toString());
    root.setString(ROUTE_F, route.toString());
    root.setString(SESSION_F, address.getSessionName());
    root.setBool(USERETRY_F, msg.getRetryEnabled());
    root.setLong(RETRY_F, msg.getRetry());
    root.setLong(TIMELEFT_F, timeRemaining);
    root.setString(PROTOCOL_F, msg.getProtocol());
    root.setLong(TRACELEVEL_F, traceLevel);
    filler.fill(BLOB_F, root);

    OutputBuf buf(8192);
    vespalib::slime::BinaryFormat::encode(slime, buf);

    size_t unCompressedSize = buf.getBuf().getDataLen();
    args.AddInt8(0);
    args.AddInt32(buf.getBuf().getDataLen());
    args.AddData(buf.getBuf().stealBuffer(), unCompressedSize);
}

std::unique_ptr<Reply>
RPCSendV2::createReply(const FRT_Values & ret, const string & serviceName, Error & error, vespalib::TraceNode & rootTrace) const
{
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

    Reply::UP reply;
    if (payloadLen > 0) {
        reply = decode(protocolName, version, BlobRef(payload, payloadLen), error);
    }
    if ( ! reply ) {
        reply.reset(new EmptyReply());
    }
    reply->setRetryDelay(retryDelay);
    for (uint32_t i = 0; i < errorCodesLen && i < errorMessagesLen && i < errorServicesLen; ++i) {
        reply->addError(Error(errorCodes[i], errorMessages[i]._str,
                              errorServices[i]._len > 0 ? errorServices[i]._str : serviceName.c_str()));
    }
    rootTrace.addChild(TraceNode::decode(trace));
    return reply;
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
