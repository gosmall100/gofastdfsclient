# gofastdfsclient
link fastdfs client

#### 1. gofastdfsclent 文件上传，文件下载， 文件删除

1.1 文件上传
```go

client, err := gofastdfsclient.NewClientWithConfig(&gofastdfsclient.Config{
    TrackerAddr: []string{"127.0.0.1:22122"},
    MaxConns:    200,
})

if err != nil {
    fmt.Println("打开fast客户端失败", err.Error())
}

defer client.Destory()
fileId, err := client.UploadByFilename(fileName)
if err != nil {
    fmt.Println("上传文件失败", err.Error())
}

fmt.Println("上传成功的文件地址：", fileId)

```
1.2 文件下载
```go

client, err := gofastdfsclient.NewClientWithConfig(&gofastdfsclient.Config{
    TrackerAddr: []string{"127.0.0.1:22122"},
    MaxConns:    200,
})
if err != nil {
    fmt.Println("打开fast客户端失败", err.Error())
}
defer client.Destory()
if err = client.DownloadToFile(fileId, tempFile, 0, 0); err != nil {
    fmt.Println("下载文件失败", err.Error())
}
```
1.3 文件删除
```go

client, err := gofastdfsclient.NewClientWithConfig(&gofastdfsclient.Config{
    TrackerAddr: []string{"127.0.0.1:22122"},
    MaxConns:    200,
})
if err != nil {
    fmt.Println("打开fast客户端失败", err.Error())
}
defer client.Destory()
    if err = client.DeleteFile(fileId); err != nil {
    fmt.Println("删除文件失败", err.Error())
}
```
