package gofastdfsclient

import (
	"fmt"
	"net"
	"sync"
)

type Client struct {
	trackerPools    map[string]*connPool
	storagePools    map[string]*connPool
	storagePoolLock *sync.RWMutex
	config          *Config
}

// NewClientWithConfig fastdfs client
func NewClientWithConfig(conf *Config) (*Client, error) {
	client := &Client{
		config:          conf,
		storagePoolLock: &sync.RWMutex{},
	}
	client.trackerPools = make(map[string]*connPool)
	client.storagePools = make(map[string]*connPool)

	for _, addr := range conf.TrackerAddr {
		trackerPool, err := newConnPool(addr, conf.MaxConns)
		if err != nil {
			return nil, err
		}
		client.trackerPools[addr] = trackerPool
	}

	return client, nil
}

func (s *Client) Destory() {
	if s == nil {
		return
	}

	for _, pool := range s.trackerPools {
		pool.Destory()
	}

	for _, pool := range s.storagePools {
		pool.Destory()
	}
}

func (s *Client) UploadByFilename(fileName string) (string, error) {
	fileinfo, err := newFileInfo(fileName, nil, "")
	defer fileinfo.Close()
	if err != nil {
		return "", err
	}

	storageinfo, err := s.queryStorageInfoWithTracker(TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITHOUT_GROUP_ONE, "", "")
	if err != nil {
		return "", err
	}

	tk := &storageUploadTask{}
	//req
	tk.fileInfo = fileinfo
	tk.storagePathIndex = storageinfo.storagePathIndex

	if err := s.doStorage(tk, storageinfo); err != nil {
		return "", err
	}
	return tk.fileId, nil
}

func (s *Client) UploadByBuffer(buffer []byte, fileExtName string) (string, error) {
	fileinfo, err := newFileInfo("", buffer, fileExtName)
	defer fileinfo.Close()
	if err != nil {
		return "", err
	}
	storageinfo, err := s.queryStorageInfoWithTracker(TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITHOUT_GROUP_ONE, "", "")
	if err != nil {
		return "", err
	}

	tk := &storageUploadTask{}
	//req
	tk.fileInfo = fileinfo
	tk.storagePathIndex = storageinfo.storagePathIndex

	if err := s.doStorage(tk, storageinfo); err != nil {
		return "", err
	}
	return tk.fileId, nil
}

func (s *Client) DownloadToFile(fileId string, localFilename string, offset int64, downloadBytes int64) error {
	groupName, remoteFilename, err := splitFileId(fileId)
	if err != nil {
		return err
	}
	// get storage server
	storageinfo, err := s.queryStorageInfoWithTracker(TRACKER_PROTO_CMD_SERVICE_QUERY_FETCH_ONE, groupName, remoteFilename)
	if err != nil {
		return err
	}

	tk := &storageDownloadTask{}
	//req
	tk.groupName = groupName
	tk.remoteFilename = remoteFilename
	tk.offset = offset
	tk.downloadBytes = downloadBytes

	//res
	tk.localFilename = localFilename

	return s.doStorage(tk, storageinfo)
}

// deprecated
func (s *Client) DownloadToBuffer(fileId string, offset int64, downloadBytes int64) ([]byte, error) {
	groupName, remoteFilename, err := splitFileId(fileId)
	if err != nil {
		return nil, err
	}
	storageinfo, err := s.queryStorageInfoWithTracker(TRACKER_PROTO_CMD_SERVICE_QUERY_FETCH_ONE, groupName, remoteFilename)
	if err != nil {
		return nil, err
	}

	tk := &storageDownloadTask{}
	//req
	tk.groupName = groupName
	tk.remoteFilename = remoteFilename
	tk.offset = offset
	tk.downloadBytes = downloadBytes

	if err := s.doStorage(tk, storageinfo); err != nil {
		return nil, err
	}
	return tk.buffer, nil
}

func (s *Client) DownloadToAllocatedBuffer(fileId string, buffer []byte, offset int64, downloadBytes int64) error {
	groupName, remoteFilename, err := splitFileId(fileId)
	if err != nil {
		return err
	}

	storageinfo, err := s.queryStorageInfoWithTracker(TRACKER_PROTO_CMD_SERVICE_QUERY_FETCH_ONE, groupName, remoteFilename)
	if err != nil {
		return err
	}

	tk := &storageDownloadTask{}
	//req
	tk.groupName = groupName
	tk.remoteFilename = remoteFilename
	tk.offset = offset
	tk.downloadBytes = downloadBytes
	tk.buffer = buffer //allocate buffer by user

	//res
	if err := s.doStorage(tk, storageinfo); err != nil {
		return err
	}
	return nil
}

func (s *Client) DeleteFile(fileId string) error {
	groupName, remoteFilename, err := splitFileId(fileId)
	if err != nil {
		return err
	}
	storageinfo, err := s.queryStorageInfoWithTracker(TRACKER_PROTO_CMD_SERVICE_QUERY_FETCH_ONE, groupName, remoteFilename)
	if err != nil {
		return err
	}

	tk := &storageDeleteTask{}
	tk.groupName = groupName
	tk.remoteFilename = remoteFilename

	return s.doStorage(tk, storageinfo)
}

func (s *Client) doTracker(tk task) error {
	// get pool tracker conn
	trackerConn, err := s.getTrackerConn()
	if err != nil {
		return err
	}
	defer func() { _ = trackerConn.Close() }()

	if err := tk.SendReq(trackerConn); err != nil {
		return err
	}
	if err := tk.RecvRes(trackerConn); err != nil {
		return err
	}

	return nil
}

func (s *Client) doStorage(tk task, storageinfo *storageInfo) error {
	storageConn, err := s.getStorageConn(storageinfo)
	if err != nil {
		return err
	}
	defer func() { _ = storageConn.Close() }()

	if err := tk.SendReq(storageConn); err != nil {
		return err
	}
	if err := tk.RecvRes(storageConn); err != nil {
		return err
	}

	return nil
}

// get storageinfo
func (s *Client) queryStorageInfoWithTracker(cmd int8, groupName string, remoteFilename string) (*storageInfo, error) {
	tk := &trackerTask{}
	tk.cmd = cmd
	tk.groupName = groupName
	tk.remoteFilename = remoteFilename

	if err := s.doTracker(tk); err != nil {
		return nil, err
	}

	return &storageInfo{
		addr:             fmt.Sprintf("%s:%d", tk.ipAddr, tk.port),
		storagePathIndex: tk.storePathIndex,
	}, nil
}

// get tracker conn
func (s *Client) getTrackerConn() (net.Conn, error) {
	var trackerConn net.Conn
	var err error
	var getOne bool
	for _, trackerPool := range s.trackerPools {
		trackerConn, err = trackerPool.get()
		if err == nil {
			getOne = true
			break
		}
	}
	if getOne {
		return trackerConn, nil
	}
	if err == nil {
		return nil, fmt.Errorf("no connPool can be use")
	}
	return nil, err
}

// get storage conn
func (s *Client) getStorageConn(storageInfo *storageInfo) (net.Conn, error) {
	s.storagePoolLock.Lock()
	storagePool, ok := s.storagePools[storageInfo.addr]
	if ok {
		s.storagePoolLock.Unlock()
		return storagePool.get()
	}

	storagePool, err := newConnPool(storageInfo.addr, s.config.MaxConns)
	if err != nil {
		s.storagePoolLock.Unlock()
		return nil, err
	}

	s.storagePools[storageInfo.addr] = storagePool
	s.storagePoolLock.Unlock()

	return storagePool.get()
}

