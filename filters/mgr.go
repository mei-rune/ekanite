package filters

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"

	"github.com/boltdb/bolt"
)

var ErrBucketNotFound = errors.New("bucket isn't found")

var OpList = []string{
	"Phrase",
	"Prefix",
	"Regexp",
	"Term",
	"Wildcard",
	"DateRange",
	"NumericRange",
	"QueryString",
}

type Filter struct {
	Field  string   `json:"field,omitempty"`
	Op     string   `json:"op"`
	Values []string `json:"values"`
}

type Query struct {
	Name        string   `json:"name"`
	Description string   `json:"description,omitempty"`
	Filters     []Filter `json:"filters,omitempty"`
}

func NewHTTPServer(db *bolt.DB, name string, handler http.Handler) *HTTPServer {
	return &HTTPServer{db: db, name: []byte(name), handler: handler}
}

type HTTPServer struct {
	db      *bolt.DB
	name    []byte
	handler http.Handler
}

func (h *HTTPServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	pa := strings.Trim(r.URL.Path, "/")
	switch r.Method {
	case "GET":
		if pa == "" {
			h.List(w, r)
		} else {
			h.Read(w, r, pa)
		}
	case "POST":
		if pa != "" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			w.Write([]byte("MethodNotAllowed"))
		} else {
			h.Create(w, r)
		}
	case "DELETE":
		if pa == "" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			w.Write([]byte("MethodNotAllowed"))
		} else {
			h.Delete(w, r, pa)
		}
	case "PUT":
		if pa == "" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			w.Write([]byte("MethodNotAllowed"))
		} else {
			h.Update(w, r, pa)
		}
	default:
		h.handler.ServeHTTP(w, r)
	}
}

func (h *HTTPServer) List(w http.ResponseWriter, r *http.Request) {
	var rs [][]byte
	err := h.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(h.name)
		if bkt == nil {
			return ErrBucketNotFound
		}

		return bkt.ForEach(func(k, v []byte) error {
			rs = append(rs, v)
			return nil
		})
	})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("["))
	for idx, bs := range rs {
		if idx != 0 {
			w.Write([]byte(","))
		}
		w.Write(bs)
	}
	w.Write([]byte("]"))
}

func (h *HTTPServer) ListID(w http.ResponseWriter, r *http.Request) {
	var rs []string
	err := h.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(h.name)
		if bkt == nil {
			return ErrBucketNotFound
		}

		return bkt.ForEach(func(k, v []byte) error {
			rs = append(rs, string(k))
			return nil
		})
	})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(rs)
}

func (h *HTTPServer) Read(w http.ResponseWriter, r *http.Request, id string) {
	var bs []byte
	err := h.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(h.name)
		if bkt == nil {
			return ErrBucketNotFound
		}

		bs = bkt.Get([]byte(id))
		return nil
	})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write(bs)
}

func (h *HTTPServer) Create(w http.ResponseWriter, r *http.Request) {
	bs, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}

	err = h.db.Update(func(tx *bolt.Tx) error {
		if !tx.Writable() {
			return bolt.ErrTxNotWritable
		}

		bkt := tx.Bucket(h.name)
		if bkt == nil {
			return ErrBucketNotFound
		}

		// Generate an ID for the new user.
		id, err := bkt.NextSequence()
		if err != nil {
			return err
		}
		if err = bkt.Put([]byte(strconv.FormatUint(id, 10)), bs); err != nil {
			return err
		}
		// delete data
		return nil
	})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte("OK"))
}

func (h *HTTPServer) Delete(w http.ResponseWriter, r *http.Request, id string) {
	err := h.db.Update(func(tx *bolt.Tx) error {
		if !tx.Writable() {
			return bolt.ErrTxNotWritable
		}

		bkt := tx.Bucket(h.name)
		if bkt == nil {
			return ErrBucketNotFound
		}
		// delete data
		return bkt.Delete([]byte(id))
	})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func (h *HTTPServer) Update(w http.ResponseWriter, r *http.Request, id string) {
	bs, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}

	err = h.db.Update(func(tx *bolt.Tx) error {
		if !tx.Writable() {
			return bolt.ErrTxNotWritable
		}

		bkt := tx.Bucket(h.name)
		if bkt == nil {
			return ErrBucketNotFound
		}
		return bkt.Put([]byte(id), bs)
	})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte("OK"))
}
