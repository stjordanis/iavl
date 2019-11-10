// Code generated by protoc-gen-go. DO NOT EDIT.
// source: proto/iavl_api.proto

package proto

import (
	context "context"
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	_ "google.golang.org/genproto/googleapis/api/annotations"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	math "math"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion3 // please upgrade the proto package

type PingRequest struct {
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *PingRequest) Reset()         { *m = PingRequest{} }
func (m *PingRequest) String() string { return proto.CompactTextString(m) }
func (*PingRequest) ProtoMessage()    {}
func (*PingRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_92d8372b52373ba9, []int{0}
}

func (m *PingRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_PingRequest.Unmarshal(m, b)
}
func (m *PingRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_PingRequest.Marshal(b, m, deterministic)
}
func (m *PingRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_PingRequest.Merge(m, src)
}
func (m *PingRequest) XXX_Size() int {
	return xxx_messageInfo_PingRequest.Size(m)
}
func (m *PingRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_PingRequest.DiscardUnknown(m)
}

var xxx_messageInfo_PingRequest proto.InternalMessageInfo

type PongResponse struct {
	Reply                string   `protobuf:"bytes,1,opt,name=reply,proto3" json:"reply,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *PongResponse) Reset()         { *m = PongResponse{} }
func (m *PongResponse) String() string { return proto.CompactTextString(m) }
func (*PongResponse) ProtoMessage()    {}
func (*PongResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_92d8372b52373ba9, []int{1}
}

func (m *PongResponse) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_PongResponse.Unmarshal(m, b)
}
func (m *PongResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_PongResponse.Marshal(b, m, deterministic)
}
func (m *PongResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_PongResponse.Merge(m, src)
}
func (m *PongResponse) XXX_Size() int {
	return xxx_messageInfo_PongResponse.Size(m)
}
func (m *PongResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_PongResponse.DiscardUnknown(m)
}

var xxx_messageInfo_PongResponse proto.InternalMessageInfo

func (m *PongResponse) GetReply() string {
	if m != nil {
		return m.Reply
	}
	return ""
}

func init() {
	proto.RegisterType((*PingRequest)(nil), "proto.PingRequest")
	proto.RegisterType((*PongResponse)(nil), "proto.PongResponse")
}

func init() { proto.RegisterFile("proto/iavl_api.proto", fileDescriptor_92d8372b52373ba9) }

var fileDescriptor_92d8372b52373ba9 = []byte{
	// 177 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0x12, 0x29, 0x28, 0xca, 0x2f,
	0xc9, 0xd7, 0xcf, 0x4c, 0x2c, 0xcb, 0x89, 0x4f, 0x2c, 0xc8, 0xd4, 0x03, 0x73, 0x85, 0x58, 0xc1,
	0x94, 0x94, 0x4c, 0x7a, 0x7e, 0x7e, 0x7a, 0x4e, 0xaa, 0x7e, 0x62, 0x41, 0xa6, 0x7e, 0x62, 0x5e,
	0x5e, 0x7e, 0x49, 0x62, 0x49, 0x66, 0x7e, 0x5e, 0x31, 0x44, 0x91, 0x12, 0x2f, 0x17, 0x77, 0x40,
	0x66, 0x5e, 0x7a, 0x50, 0x6a, 0x61, 0x69, 0x6a, 0x71, 0x89, 0x92, 0x0a, 0x17, 0x4f, 0x40, 0x3e,
	0x88, 0x5b, 0x5c, 0x90, 0x9f, 0x57, 0x9c, 0x2a, 0x24, 0xc2, 0xc5, 0x5a, 0x94, 0x5a, 0x90, 0x53,
	0x29, 0xc1, 0xa8, 0xc0, 0xa8, 0xc1, 0x19, 0x04, 0xe1, 0x18, 0x05, 0x70, 0x71, 0x7b, 0x3a, 0x86,
	0xf9, 0x04, 0xa7, 0x16, 0x95, 0x65, 0x26, 0xa7, 0x0a, 0x39, 0x72, 0xb1, 0x80, 0xcc, 0x10, 0x12,
	0x82, 0x98, 0xa9, 0x87, 0x64, 0xa0, 0x94, 0x30, 0x4c, 0x0c, 0xc9, 0x54, 0x25, 0x81, 0xa6, 0xcb,
	0x4f, 0x26, 0x33, 0x71, 0x09, 0x71, 0xe8, 0x97, 0x19, 0xea, 0x17, 0x64, 0xe6, 0xa5, 0x27, 0xb1,
	0x81, 0x55, 0x19, 0x03, 0x02, 0x00, 0x00, 0xff, 0xff, 0x8f, 0x4a, 0x93, 0x36, 0xca, 0x00, 0x00,
	0x00,
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// IAVLServiceClient is the client API for IAVLService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type IAVLServiceClient interface {
	Ping(ctx context.Context, in *PingRequest, opts ...grpc.CallOption) (*PongResponse, error)
}

type iAVLServiceClient struct {
	cc *grpc.ClientConn
}

func NewIAVLServiceClient(cc *grpc.ClientConn) IAVLServiceClient {
	return &iAVLServiceClient{cc}
}

func (c *iAVLServiceClient) Ping(ctx context.Context, in *PingRequest, opts ...grpc.CallOption) (*PongResponse, error) {
	out := new(PongResponse)
	err := c.cc.Invoke(ctx, "/proto.IAVLService/Ping", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// IAVLServiceServer is the server API for IAVLService service.
type IAVLServiceServer interface {
	Ping(context.Context, *PingRequest) (*PongResponse, error)
}

// UnimplementedIAVLServiceServer can be embedded to have forward compatible implementations.
type UnimplementedIAVLServiceServer struct {
}

func (*UnimplementedIAVLServiceServer) Ping(ctx context.Context, req *PingRequest) (*PongResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Ping not implemented")
}

func RegisterIAVLServiceServer(s *grpc.Server, srv IAVLServiceServer) {
	s.RegisterService(&_IAVLService_serviceDesc, srv)
}

func _IAVLService_Ping_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PingRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(IAVLServiceServer).Ping(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/proto.IAVLService/Ping",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(IAVLServiceServer).Ping(ctx, req.(*PingRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var _IAVLService_serviceDesc = grpc.ServiceDesc{
	ServiceName: "proto.IAVLService",
	HandlerType: (*IAVLServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Ping",
			Handler:    _IAVLService_Ping_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "proto/iavl_api.proto",
}