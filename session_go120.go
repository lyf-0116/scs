//go:build go1.20
// +build go1.20

package scs

import (
	"bufio"
	"net"
	"net/http"
)

func (sw *sessionResponseWriter) Flush() {
	err := sw.commitBeforeWrite()
	if err != nil {
		sw.sessionManager.ErrorFunc(sw.ResponseWriter, sw.request, err)
	}
	http.NewResponseController(sw.ResponseWriter).Flush()
}

func (sw *sessionResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	err := sw.commitBeforeWrite()
	if err != nil {
		sw.sessionManager.ErrorFunc(sw.ResponseWriter, sw.request, err)
	}
	return http.NewResponseController(sw.ResponseWriter).Hijack()
}
