package http

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"

	"github.com/runner-mei/borm"
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
	ID          string   `json:"id,omitempty"`
	Name        string   `json:"name"`
	Description string   `json:"description,omitempty"`
	Filters     []Filter `json:"filters,omitempty"`
}

type filterServer struct {
	db *borm.Bucket
}

func (h *filterServer) List(w http.ResponseWriter, r *http.Request) {
	var rs []Query
	err := h.db.ForEach(func(it *borm.Iterator) error {
		for it.Next() {
			var q Query
			if err := it.Read(&q); err != nil {
				return err
			}
			q.ID = string(it.Key())
			rs = append(rs, q)
		}
		return nil
	})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	w.WriteHeader(http.StatusOK)
	encodeJSON(w, rs)
}

func (h *filterServer) ListID(w http.ResponseWriter, r *http.Request) {
	var rs []Query
	err := h.db.ForEach(func(it *borm.Iterator) error {
		for it.Next() {
			var q Query
			if err := it.Read(&q); err != nil {
				return err
			}
			q.ID = string(it.Key())
			q.Filters = nil
			rs = append(rs, q)
		}
		return nil
	})

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	w.WriteHeader(http.StatusOK)
	encodeJSON(w, rs)
}

func (h *filterServer) Read(w http.ResponseWriter, r *http.Request, id string) {
	var q Query
	err := h.db.Get(id, &q)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	w.WriteHeader(http.StatusOK)
	encodeJSON(w, &q)
}

func (h *filterServer) Create(w http.ResponseWriter, r *http.Request) {
	bs, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}
	var q Query
	if err := json.Unmarshal(bs, &q); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}

	err = h.db.Insert(borm.GenerateID(), &q)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte("OK"))
}

func (h *filterServer) Delete(w http.ResponseWriter, r *http.Request, id string) {
	err := h.db.Delete(id)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func (h *filterServer) Update(w http.ResponseWriter, r *http.Request, id string) {
	bs, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}

	var q Query
	if err := json.Unmarshal(bs, &q); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}

	err = h.db.Upsert(id, &q)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte("OK"))
}
