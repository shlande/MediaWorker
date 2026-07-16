package baidu

// PCS API response types used by Baidu Driver.

type pcsListResponse struct {
	Errno  int            `json:"errno"`
	List   []pcsFileEntry `json:"list"`
	Errmsg string         `json:"errmsg,omitempty"`
}

type pcsFileEntry struct {
	FsID        int64  `json:"fs_id"`
	Filename    string `json:"filename"`
	Size        int64  `json:"size"`
	IsDir       int    `json:"isdir"`
	ServerMtime int64  `json:"server_mtime"`
	MD5         string `json:"md5"`
}

type pcsMetaResponse struct {
	Errno int            `json:"errno"`
	List  []pcsMetaEntry `json:"list"`
}

type pcsMetaEntry struct {
	FsID        int64  `json:"fs_id"`
	Filename    string `json:"filename"`
	Size        int64  `json:"size"`
	IsDir       int    `json:"isdir"`
	ServerMtime int64  `json:"server_mtime"`
	MD5         string `json:"md5"`
	Dlink       string `json:"dlink,omitempty"`
}

type pcsPrecreateResponse struct {
	Errno    int    `json:"errno"`
	UploadID string `json:"uploadid"`
}

type pcsUploadResponse struct {
	Errno int    `json:"errno"`
	MD5   string `json:"md5"`
}

type pcsCreateResponse struct {
	Errno int            `json:"errno"`
	Info  *pcsCreateInfo `json:"info"`
}

type pcsCreateInfo struct {
	FsID        int64  `json:"fs_id"`
	Path        string `json:"path"`
	Size        int64  `json:"size"`
	IsDir       int    `json:"isdir"`
	MD5         string `json:"md5"`
	ServerMtime int64  `json:"server_mtime"`
}

type pcsDeleteResponse struct {
	Errno int `json:"errno"`
}

type pcsUInfoResponse struct {
	Errno int `json:"errno"`
}
