package gofastdfsclient

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"strings"
)

const (
	TRACKER_PROTO_CMD_RESP                                  = 100 // tracker 响应码
	TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITHOUT_GROUP_ONE = 101 // 获取一个 storage server 用来存储文件（不指定 group name）
	TRACKER_PROTO_CMD_SERVICE_QUERY_FETCH_ONE               = 102 // 获取一个 storage server 用来下载文件

	STORAGE_PROTO_CMD_UPLOAD_FILE   = 11  // 上传普通文件
	STORAGE_PROTO_CMD_DELETE_FILE   = 12  // 删除文件
	STORAGE_PROTO_CMD_DOWNLOAD_FILE = 14  // 下载文件
	FDFS_PROTO_CMD_ACTIVE_TEST      = 111 // 激活测试

	FDFS_GROUP_NAME_MAX_LEN = 16
)

type storageInfo struct {
	addr             string
	storagePathIndex int8
}

type fileInfo struct {
	fileSize    int64
	buffer      []byte
	file        *os.File
	fileExtName string
}

func newFileInfo(fileName string, buffer []byte, fileExtName string) (*fileInfo, error) {
	if fileName != "" {
		file, err := os.Open(fileName)
		if err != nil {
			return nil, err
		}
		stat, err := file.Stat()
		if err != nil {
			return nil, err
		}
		if int(stat.Size()) == 0 {
			return nil, fmt.Errorf("file %q size is zero", fileName)
		}

		index := strings.LastIndexByte(fileName, '.')
		if index != -1 {
			fileExtName = fileName[index+1:]
		}

		return &fileInfo{
			fileSize:    stat.Size(),
			file:        file,
			fileExtName: fileExtName,
		}, nil
	}

	if len(fileExtName) > 6 {
		fileExtName = fileExtName[:6]
	}

	return &fileInfo{
		fileSize:    int64(len(buffer)),
		buffer:      buffer,
		fileExtName: fileExtName,
	}, nil
}

func (t *fileInfo) Close() {
	if t == nil {
		return
	}
	if t.file != nil {
		_ = t.file.Close()
	}
	return
}

type task interface {
	SendReq(net.Conn) error
	RecvRes(net.Conn) error
}

// The header only has 10 bytes
type header struct {
	cmd    int8  // 1-byte integer, command code
	status int8  // 1 byte integer, status code, 0 indicates success, non 0 indicates failure
	pkgLen int64 // 8-byte integer, body length, excluding header, only the length of the body
}

func (h *header) SendHeader(conn net.Conn) error {
	buffer := new(bytes.Buffer)
	if err := binary.Write(buffer, binary.BigEndian, h.pkgLen); err != nil {
		return err
	}
	buffer.WriteByte(byte(h.cmd))
	buffer.WriteByte(byte(h.status))

	// send conn
	if _, err := conn.Write(buffer.Bytes()); err != nil {
		return err
	}
	return nil
}

func (h *header) RecvHeader(conn net.Conn) error {
	// read header
	buf := make([]byte, 10)
	if _, err := conn.Read(buf); err != nil {
		return err
	}

	buffer := bytes.NewBuffer(buf)
	if err := binary.Read(buffer, binary.BigEndian, &h.pkgLen); err != nil {
		return err
	}

	cmd, err := buffer.ReadByte()
	if err != nil {
		return err
	}

	status, err := buffer.ReadByte()
	if err != nil {
		return err
	}

	if status != 0 {
		return fmt.Errorf("recv resp status %d != 0", status)
	}
	
	h.cmd = int8(cmd)
	h.status = int8(status)
	return nil
}

func splitFileId(fileId string) (string, string, error) {
	str := strings.SplitN(fileId, "/", 2)
	if len(str) < 2 {
		return "", "", fmt.Errorf("invalid fildId")
	}
	return str[0], str[1], nil
}

