package http

import (
	"encoding/json"
	"io/ioutil"
	"net/http"

	"github.com/ekanite/ekanite/service"
)

func (h *Server) ListFilters(w http.ResponseWriter, r *http.Request) {
	rs := h.metaStore.ListQueries()

	w.WriteHeader(http.StatusOK)
	renderJSON(w, rs)
}

func (h *Server) ListFilterIDs(w http.ResponseWriter, r *http.Request) {
	rs, err := h.metaStore.ListQueryIDs()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}

	w.WriteHeader(http.StatusOK)
	renderJSON(w, rs)
}

func (h *Server) ReadFilter(w http.ResponseWriter, r *http.Request, id string) {
	q, err := h.metaStore.ReadQuery(id)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}

	w.WriteHeader(http.StatusOK)
	renderJSON(w, &q)
}

func (s *Server) CreateFilter(w http.ResponseWriter, r *http.Request) {
	bs, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}
	var q service.Query
	if err := json.Unmarshal(bs, &q); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}

	_, err = s.metaStore.CreateQuery(q)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte("OK"))
}

func (h *Server) DeleteFilter(w http.ResponseWriter, r *http.Request, id string) {
	err := h.metaStore.DeleteQuery(id)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func (s *Server) UpdateFilter(w http.ResponseWriter, r *http.Request, id string) {
	bs, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}

	var q service.Query
	if err := json.Unmarshal(bs, &q); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}

	err = s.metaStore.UpdateQuery(id, q)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte("OK"))
}
