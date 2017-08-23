package utils

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"strconv"
	"strings"
        "bytes"
)

func Gzip(data []byte) []byte {
    var res bytes.Buffer
    gz, _ := gzip.NewWriterLevel(&res, 7)
    _, err := gz.Write(data)
    if err != nil {
        panic(err)
    } else {
        gz.Close()
    }
    return res.Bytes()
}

func Extract_File(fr string) (r map[string][]byte) {
	r = make(map[string][]byte)
	b := strings.NewReader(fr)
	gr, err := gzip.NewReader(b)
	if err != nil {
		panic(err)
	}
	defer gr.Close()
	tr := tar.NewReader(gr)
	for {
		hdr, err := tr.Next()
		if err != nil {
			break
		}
		bfRd := bufio.NewReader(tr)
		for {
			data := make([]byte, 50000)
			n, err := bfRd.Read(data)
			if err != nil { //遇到任何错误立即返回，并忽略 EOF 错误信息
				break
			}
			if n == 0 {
				continue
			}
			r[hdr.Name] = data[:n]
		}
	}
	return
}

func Process_file(data map[string][]byte, pending map[string][]string) {
	for key := range data {
                var datanum = 0
		content := string(data[key])
		fields := strings.Split(content, "\n")
		seq := make([]string, len(fields))
		args := strings.Split(key, "/")
		name, ctg := args[0], args[1]
		names := strings.Split(name, "_")
		mac, stamp, version := names[1], names[2], names[3]
		for i, v := range fields {
			if v != "" {
				seq[i] = mac + ", " + v
                                datanum++
			}
		}
                seq = seq[:datanum]
		_stamp, err := strconv.Atoi(stamp[10:12])
		if err != nil {
			continue
		}
		d := strconv.Itoa(_stamp / 5 * 5)
		var s string
		if len(d) == 1 {
			s = "0" + d
		} else {
			s = d
		}
		stamp = stamp[:10] + s
		key = version + "/" + stamp + "/" + strings.ToLower(ctg)

		_seq, ok := pending[key]
		if ok {
			pending[key] = Merge(_seq, seq)
		} else {
			pending[key] = seq
		}
	}
        pending = make(map[string][]string)
}
